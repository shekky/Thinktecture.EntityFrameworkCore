using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Internal;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.Logging;
using Thinktecture.EntityFrameworkCore.Data;

namespace Thinktecture.EntityFrameworkCore.BulkOperations
{
   /// <summary>
   /// Executes bulk operations.
   /// </summary>
   // ReSharper disable once ClassNeverInstantiated.Global
   [SuppressMessage("ReSharper", "EF1001")]
   public class SqliteBulkOperationExecutor : IBulkOperationExecutor
   {
      private readonly ISqlGenerationHelper _sqlGenerationHelper;
      private readonly IDiagnosticsLogger<SqliteDbLoggerCategory.BulkOperation> _logger;

      private static class EventIds
      {
         public static readonly EventId Inserting = 0;
         public static readonly EventId Inserted = 1;
      }

      /// <summary>
      /// Initializes new instance of <see cref="SqliteBulkOperationExecutor"/>.
      /// </summary>
      /// <param name="sqlGenerationHelper">SQL generation helper.</param>
      /// <param name="logger"></param>
      public SqliteBulkOperationExecutor([JetBrains.Annotations.NotNull] ISqlGenerationHelper sqlGenerationHelper,
                                         [JetBrains.Annotations.NotNull] IDiagnosticsLogger<SqliteDbLoggerCategory.BulkOperation> logger)
      {
         _sqlGenerationHelper = sqlGenerationHelper ?? throw new ArgumentNullException(nameof(sqlGenerationHelper));
         _logger = logger ?? throw new ArgumentNullException(nameof(logger));
      }

      /// <inheritdoc />
      public IBulkInsertOptions CreateOptions()
      {
         return new SqliteBulkInsertOptions();
      }

      /// <inheritdoc />
      public Task BulkInsertAsync<T>(DbContext ctx,
                                     IEntityType entityType,
                                     IEnumerable<T> entities,
                                     IBulkInsertOptions options,
                                     CancellationToken cancellationToken = default)
         where T : class
      {
         if (entityType == null)
            throw new ArgumentNullException(nameof(entityType));

         return BulkInsertAsync(ctx, entityType, entities, entityType.GetSchema(), entityType.GetTableName(), options, cancellationToken);
      }

      /// <inheritdoc />
      public async Task BulkInsertAsync<T>(DbContext ctx,
                                           IEntityType entityType,
                                           IEnumerable<T> entities,
                                           string schema,
                                           string tableName,
                                           IBulkInsertOptions options,
                                           CancellationToken cancellationToken = default)
         where T : class
      {
         if (ctx == null)
            throw new ArgumentNullException(nameof(ctx));
         if (entities == null)
            throw new ArgumentNullException(nameof(entities));
         if (tableName == null)
            throw new ArgumentNullException(nameof(tableName));
         if (options == null)
            throw new ArgumentNullException(nameof(options));

         if (!(options is SqliteBulkInsertOptions sqliteOptions))
            throw new ArgumentException($"'{nameof(SqliteBulkOperationExecutor)}' expected options of type '{nameof(SqliteBulkInsertOptions)}' but found '{options.GetType().DisplayName()}'.", nameof(options));

         var factory = ctx.GetService<IEntityDataReaderFactory>();
         var properties = GetPropertiesForInsert(sqliteOptions.EntityMembersProvider, entityType);
         var sqlCon = (SqliteConnection)ctx.Database.GetDbConnection();

         using (var reader = factory.Create(ctx, entities, properties))
         {
            await ctx.Database.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);

            try
            {
               using (var command = sqlCon.CreateCommand())
               {
                  var tableIdentifier = _sqlGenerationHelper.DelimitIdentifier(tableName, schema);
#pragma warning disable CA2100
                  command.CommandText = GetInsertStatement(reader, tableIdentifier);
#pragma warning restore CA2100
                  var parameterInfos = CreateParameters(reader, command);

                  try
                  {
                     command.Prepare();
                  }
                  catch (SqliteException ex)
                  {
                     throw new InvalidOperationException($"Cannot access destination table '{tableIdentifier}'.", ex);
                  }

                  LogInserting(command.CommandText);
                  var stopwatch = Stopwatch.StartNew();

                  while (reader.Read())
                  {
                     for (var i = 0; i < reader.FieldCount; i++)
                     {
                        var paramInfo = parameterInfos[i];
                        var value = reader.GetValue(i);

                        if (sqliteOptions.AutoIncrementBehavior == SqliteAutoIncrementBehavior.SetZeroToNull && paramInfo.IsAutoIncrementColumn && value.Equals(0))
                           value = null;

                        paramInfo.Parameter.Value = value ?? DBNull.Value;
                     }

                     await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                  }

                  stopwatch.Stop();
                  LogInserted(command.CommandText, stopwatch.Elapsed);
               }
            }
            finally
            {
               ctx.Database.CloseConnection();
            }
         }
      }

      [JetBrains.Annotations.NotNull]
      private static ParameterInfo[] CreateParameters([JetBrains.Annotations.NotNull] IEntityDataReader reader, SqliteCommand command)
      {
         var parameters = new ParameterInfo[reader.Properties.Count];

         for (var i = 0; i < reader.Properties.Count; i++)
         {
            var property = reader.Properties[i];
            var index = reader.GetPropertyIndex(property);

            var parameter = command.CreateParameter();
            parameter.ParameterName = $"$p{index}";
            parameters[i] = new ParameterInfo(parameter, IsAutoIncrement(property));
            command.Parameters.Add(parameter);
         }

         return parameters;
      }

      [JetBrains.Annotations.NotNull]
      private string GetInsertStatement([JetBrains.Annotations.NotNull] IEntityDataReader reader,
                                        [JetBrains.Annotations.NotNull] string tableIdentifier)
      {
         var sb = new StringBuilder();
         sb.Append("INSERT INTO ").Append(tableIdentifier).Append("(");

         for (var i = 0; i < reader.Properties.Count; i++)
         {
            var column = reader.Properties[i];

            if (i > 0)
               sb.Append(", ");

            sb.Append(_sqlGenerationHelper.DelimitIdentifier(column.GetColumnName()));
         }

         sb.Append(") VALUES (");

         for (var i = 0; i < reader.Properties.Count; i++)
         {
            var property = reader.Properties[i];
            var index = reader.GetPropertyIndex(property);

            if (i > 0)
               sb.Append(", ");

            sb.Append("$p").Append(index);
         }

         sb.Append(");");

         return sb.ToString();
      }

      private static bool IsAutoIncrement([JetBrains.Annotations.NotNull] IProperty property)
      {
         return property.ValueGenerated == ValueGenerated.OnAdd
                && (property.ClrType == typeof(int) || property.ClrType == typeof(int?))
                && property.FindTypeMapping()?.Converter == null;
      }

      private void LogInserting(string insertStatement)
      {
         _logger.Logger.LogInformation(EventIds.Inserting, @"Executing DbCommand
{insertStatement}", insertStatement);
      }

      private void LogInserted(string insertStatement, TimeSpan duration)
      {
         _logger.Logger.LogInformation(EventIds.Inserted, @"Executed DbCommand ({duration}ms)
{insertStatement}", (long)duration.TotalMilliseconds, insertStatement);
      }

      [JetBrains.Annotations.NotNull]
      private static IReadOnlyList<IProperty> GetPropertiesForInsert([CanBeNull] IEntityMembersProvider entityMembersProvider,
                                                                     [JetBrains.Annotations.NotNull] IEntityType entityType)
      {
         if (entityMembersProvider == null)
            return entityType.GetProperties().Where(p => p.GetBeforeSaveBehavior() != PropertySaveBehavior.Ignore).ToList();

         return ConvertToEntityProperties(entityMembersProvider.GetMembers(), entityType);
      }

      [JetBrains.Annotations.NotNull]
      private static IReadOnlyList<IProperty> ConvertToEntityProperties([JetBrains.Annotations.NotNull] IReadOnlyList<MemberInfo> memberInfos, [JetBrains.Annotations.NotNull] IEntityType entityType)
      {
         var properties = new IProperty[memberInfos.Count];

         for (var i = 0; i < memberInfos.Count; i++)
         {
            var memberInfo = memberInfos[i];
            var property = FindProperty(entityType, memberInfo);

            properties[i] = property ?? throw new ArgumentException($"The member '{memberInfo.Name}' has not found on entity '{entityType.Name}'.", nameof(memberInfos));
         }

         return properties;
      }

      [CanBeNull]
      private static IProperty FindProperty([JetBrains.Annotations.NotNull] IEntityType entityType, [JetBrains.Annotations.NotNull] MemberInfo memberInfo)
      {
         foreach (var property in entityType.GetProperties())
         {
            if (property.PropertyInfo == memberInfo || property.FieldInfo == memberInfo)
               return property;
         }

         return null;
      }

      private readonly struct ParameterInfo
      {
         public readonly SqliteParameter Parameter;
         public readonly bool IsAutoIncrementColumn;

         public ParameterInfo([JetBrains.Annotations.NotNull] SqliteParameter parameter, bool isAutoIncrementColumn)
         {
            Parameter = parameter;
            IsAutoIncrementColumn = isAutoIncrementColumn;
         }
      }
   }
}
