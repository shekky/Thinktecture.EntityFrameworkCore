using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.Logging;
using Thinktecture.EntityFrameworkCore.Data;
using Thinktecture.EntityFrameworkCore.TempTables;

namespace Thinktecture.EntityFrameworkCore.BulkOperations
{
   /// <summary>
   /// Executes bulk operations.
   /// </summary>
   // ReSharper disable once ClassNeverInstantiated.Global
   [SuppressMessage("ReSharper", "EF1001")]
   public sealed class SqliteBulkOperationExecutor : IBulkOperationExecutor, ITempTableBulkOperationExecutor
   {
      private readonly DbContext _ctx;
      private readonly IDiagnosticsLogger<SqliteDbLoggerCategory.BulkOperation> _logger;
      private readonly ISqlGenerationHelper _sqlGenerationHelper;

      private static class EventIds
      {
         public static readonly EventId Inserting = 0;
         public static readonly EventId Inserted = 1;
      }

      /// <summary>
      /// Initializes new instance of <see cref="SqliteBulkOperationExecutor"/>.
      /// </summary>
      /// <param name="ctx">Current database context.</param>
      /// <param name="logger">Logger.</param>
      /// <param name="sqlGenerationHelper">SQL generation helper.</param>
      public SqliteBulkOperationExecutor(
         ICurrentDbContext ctx,
         IDiagnosticsLogger<SqliteDbLoggerCategory.BulkOperation> logger,
         ISqlGenerationHelper sqlGenerationHelper)
      {
         _ctx = ctx?.Context ?? throw new ArgumentNullException(nameof(ctx));
         _logger = logger ?? throw new ArgumentNullException(nameof(logger));
         _sqlGenerationHelper = sqlGenerationHelper ?? throw new ArgumentNullException(nameof(sqlGenerationHelper));
      }

      /// <inheritdoc />
      IBulkInsertOptions IBulkOperationExecutor.CreateOptions()
      {
         return new SqliteBulkInsertOptions();
      }

      /// <inheritdoc />
      ITempTableBulkInsertOptions ITempTableBulkOperationExecutor.CreateOptions()
      {
         return new SqliteTempTableBulkInsertOptions();
      }

      /// <inheritdoc />
      public Task BulkInsertAsync<T>(
         IEnumerable<T> entities,
         IBulkInsertOptions options,
         CancellationToken cancellationToken = default)
         where T : class
      {
         var entityType = _ctx.Model.GetEntityType(typeof(T));

         return BulkInsertAsync(entityType, entities, entityType.GetSchema(), entityType.GetTableName(), options, cancellationToken);
      }

      private async Task BulkInsertAsync<T>(
         IEntityType entityType,
         IEnumerable<T> entities,
         string? schema,
         string tableName,
         IBulkInsertOptions options,
         CancellationToken cancellationToken = default)
         where T : class
      {
         if (entities == null)
            throw new ArgumentNullException(nameof(entities));
         if (tableName == null)
            throw new ArgumentNullException(nameof(tableName));
         if (options == null)
            throw new ArgumentNullException(nameof(options));

         if (!(options is SqliteBulkInsertOptions sqliteOptions))
            sqliteOptions = new SqliteBulkInsertOptions(options);

         var factory = _ctx.GetService<IEntityDataReaderFactory>();
         var properties = sqliteOptions.MembersToInsert.GetPropertiesForInsert(entityType);
         var sqlCon = (SqliteConnection)_ctx.Database.GetDbConnection();

         using var reader = factory.Create(_ctx, entities, properties);

         await _ctx.Database.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);

         try
         {
            using var command = sqlCon.CreateCommand();

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
                  object? value = reader.GetValue(i);

                  if (sqliteOptions.AutoIncrementBehavior == SqliteAutoIncrementBehavior.SetZeroToNull && paramInfo.IsAutoIncrementColumn && 0.Equals(value))
                     value = null;

                  paramInfo.Parameter.Value = value ?? DBNull.Value;
               }

               await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }

            stopwatch.Stop();
            LogInserted(command.CommandText, stopwatch.Elapsed);
         }
         finally
         {
            await _ctx.Database.CloseConnectionAsync().ConfigureAwait(false);
         }
      }

      private static ParameterInfo[] CreateParameters(IEntityDataReader reader, SqliteCommand command)
      {
         var parameters = new ParameterInfo[reader.FieldCount];

         foreach (var (index, property) in reader.GetProperties())
         {
            var parameter = command.CreateParameter();
            parameter.ParameterName = $"$p{index}";
            parameters[index] = new ParameterInfo(parameter, property.IsAutoIncrement());
            command.Parameters.Add(parameter);
         }

         return parameters;
      }

      private string GetInsertStatement(IEntityDataReader reader,
                                        string tableIdentifier)
      {
         var sb = new StringBuilder();
         sb.Append("INSERT INTO ").Append(tableIdentifier).Append("(");

         foreach (var (index, property) in reader.GetProperties())
         {
            if (index > 0)
               sb.Append(", ");

            sb.Append(_sqlGenerationHelper.DelimitIdentifier(property.GetColumnName()));
         }

         sb.Append(") VALUES (");

         foreach (var (index, _) in reader.GetProperties())
         {
            if (index > 0)
               sb.Append(", ");

            sb.Append("$p").Append(index);
         }

         sb.Append(");");

         return sb.ToString();
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

      /// <inheritdoc />
      public async Task<ITempTableQuery<T>> BulkInsertIntoTempTableAsync<T>(
         IEnumerable<T> entities,
         ITempTableBulkInsertOptions options,
         CancellationToken cancellationToken = default)
         where T : class
      {
         if (entities == null)
            throw new ArgumentNullException(nameof(entities));
         if (options == null)
            throw new ArgumentNullException(nameof(options));

         var entityType = _ctx.Model.GetEntityType(typeof(T));
         var tempTableCreator = _ctx.GetService<ITempTableCreator>();

         if (!(options is SqliteTempTableBulkInsertOptions))
         {
            var sqliteOptions = new SqliteTempTableBulkInsertOptions(options);
            options = sqliteOptions;
         }

         var tempTableReference = await tempTableCreator.CreateTempTableAsync(entityType, options.TempTableCreationOptions, cancellationToken).ConfigureAwait(false);

         try
         {
            await BulkInsertAsync(entityType, entities, null, tempTableReference.Name, options.BulkInsertOptions, cancellationToken).ConfigureAwait(false);

            var query = _ctx.Set<T>().FromSqlRaw($"SELECT * FROM {_sqlGenerationHelper.DelimitIdentifier(tempTableReference.Name)}");

            return new TempTableQuery<T>(query, tempTableReference);
         }
         catch (Exception)
         {
            await tempTableReference.DisposeAsync().ConfigureAwait(false);
            throw;
         }
      }

      private readonly struct ParameterInfo
      {
         public readonly SqliteParameter Parameter;
         public readonly bool IsAutoIncrementColumn;

         public ParameterInfo(SqliteParameter parameter, bool isAutoIncrementColumn)
         {
            Parameter = parameter;
            IsAutoIncrementColumn = isAutoIncrementColumn;
         }
      }
   }
}
