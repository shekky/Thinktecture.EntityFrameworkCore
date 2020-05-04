using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
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
   [SuppressMessage("ReSharper", "EF1001")]
   public sealed class SqlServerBulkOperationExecutor : IBulkOperationExecutor, ITempTableBulkOperationExecutor
   {
      private readonly DbContext _ctx;
      private readonly IDiagnosticsLogger<SqlServerDbLoggerCategory.BulkOperation> _logger;
      private readonly ISqlGenerationHelper _sqlGenerationHelper;

      private static class EventIds
      {
         public static readonly EventId Inserting = 0;
         public static readonly EventId Inserted = 1;
      }

      /// <summary>
      /// Initializes new instance of <see cref="SqlServerBulkOperationExecutor"/>.
      /// </summary>
      /// <param name="ctx">Current database context.</param>
      /// <param name="logger">Logger.</param>
      /// <param name="sqlGenerationHelper">SQL generation helper.</param>
      public SqlServerBulkOperationExecutor(
         ICurrentDbContext ctx,
         IDiagnosticsLogger<SqlServerDbLoggerCategory.BulkOperation> logger,
         ISqlGenerationHelper sqlGenerationHelper)
      {
         _ctx = ctx?.Context ?? throw new ArgumentNullException(nameof(ctx));
         _logger = logger ?? throw new ArgumentNullException(nameof(logger));
         _sqlGenerationHelper = sqlGenerationHelper ?? throw new ArgumentNullException(nameof(sqlGenerationHelper));
      }

      /// <inheritdoc />
      IBulkInsertOptions IBulkOperationExecutor.CreateOptions()
      {
         return new SqlServerBulkInsertOptions();
      }

      /// <inheritdoc />
      ITempTableBulkInsertOptions ITempTableBulkOperationExecutor.CreateOptions()
      {
         return new SqlServerTempTableBulkInsertOptions();
      }

      /// <inheritdoc />
      public async Task BulkInsertAsync<T>(
         IEnumerable<T> entities,
         IBulkInsertOptions options,
         CancellationToken cancellationToken = default)
         where T : class
      {
         if (entities == null)
            throw new ArgumentNullException(nameof(entities));
         if (options == null)
            throw new ArgumentNullException(nameof(options));

         var entityType = _ctx.Model.GetEntityType(typeof(T));
         var bulkInsertContext = CreateBulkInsertContext(options);

         await _ctx.Database.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);

         try
         {
            await BulkInsertAsync(entityType, entities, entityType.GetSchema(), entityType.GetTableName(), bulkInsertContext, cancellationToken).ConfigureAwait(false);
            await BulkInsertSeparatedOwnedEntitiesAsync(entityType, entities).ConfigureAwait(false);
         }
         finally
         {
            await _ctx.Database.CloseConnectionAsync().ConfigureAwait(false);
         }
      }

      private async Task BulkInsertAsync<T>(
         IEntityType entityType,
         IEnumerable<T> entities,
         string? schema,
         string tableName,
         BulkInsertContext bulkInsertContext,
         CancellationToken cancellationToken)
         where T : class
      {
         var properties = bulkInsertContext.Options.MembersToInsert.GetPropertiesForInsert(entityType);

         using var reader = bulkInsertContext.Factory.Create(_ctx, entities, properties);
         using var bulkCopy = CreateSqlBulkCopy(bulkInsertContext, schema, tableName);

         var columns = SetColumnMappings(bulkCopy, reader);

         LogInserting(bulkInsertContext.Options.SqlBulkCopyOptions, bulkCopy, columns);
         var stopwatch = Stopwatch.StartNew();

         await bulkCopy.WriteToServerAsync(reader, cancellationToken).ConfigureAwait(false);

         LogInserted(bulkInsertContext.Options.SqlBulkCopyOptions, stopwatch.Elapsed, bulkCopy, columns);
      }

      private async Task BulkInsertSeparatedOwnedEntitiesAsync<T>(
         IEntityType entityType,
         IEnumerable<T> entities)
         where T : class
      {
         foreach (var navi in entityType.GetOwnedTypesProperties(false))
         {
            if (navi.ForeignKey.IsOwnership && navi.ForeignKey.PrincipalEntityType == entityType)
            {
               var ownedEntities = GetOwnedEntities(entities, navi);
            }
         }
      }

      private static List<object> GetOwnedEntities<T>(IEnumerable<T> entities, INavigation ownedProperty)
         where T : class
      {
         var getter = ownedProperty.GetGetter() ?? throw new Exception($"No property-getter for the navigational property '{ownedProperty.ClrType.Name}.{ownedProperty.PropertyInfo.Name}' found.");

         return entities.Select(getter.GetClrValue).Where(e => e != null).ToList();
      }

      private static string SetColumnMappings(SqlBulkCopy bulkCopy, IEntityDataReader reader)
      {
         var columnsSb = new StringBuilder();

         for (var i = 0; i < reader.Properties.Count; i++)
         {
            var property = reader.Properties[i];
            var index = reader.GetPropertyIndex(property);
            var columnName = property.GetColumnName();

            bulkCopy.ColumnMappings.Add(new SqlBulkCopyColumnMapping(index, columnName));

            if (columnsSb.Length > 0)
               columnsSb.Append(", ");

            columnsSb.Append(columnName).Append(" ").Append(property.GetColumnType());
         }

         return columnsSb.ToString();
      }

      private SqlBulkCopy CreateSqlBulkCopy(
         BulkInsertContext bulkInsertContext,
         string? schema,
         string tableName)
      {
         var bulkCopy = new SqlBulkCopy(bulkInsertContext.Connection, bulkInsertContext.Options.SqlBulkCopyOptions, bulkInsertContext.Transaction)
                        {
                           DestinationTableName = _sqlGenerationHelper.DelimitIdentifier(tableName, schema),
                           EnableStreaming = bulkInsertContext.Options.EnableStreaming
                        };

         if (bulkInsertContext.Options.BulkCopyTimeout.HasValue)
            bulkCopy.BulkCopyTimeout = (int)bulkInsertContext.Options.BulkCopyTimeout.Value.TotalSeconds;

         if (bulkInsertContext.Options.BatchSize.HasValue)
            bulkCopy.BatchSize = bulkInsertContext.Options.BatchSize.Value;

         return bulkCopy;
      }

      private void LogInserting(SqlBulkCopyOptions options, SqlBulkCopy bulkCopy, string columns)
      {
         _logger.Logger.LogInformation(EventIds.Inserting, @"Executing DbCommand [SqlBulkCopyOptions={SqlBulkCopyOptions}, BulkCopyTimeout={BulkCopyTimeout}, BatchSize={BatchSize}, EnableStreaming={EnableStreaming}]
INSERT BULK {table} ({columns})", options, bulkCopy.BulkCopyTimeout, bulkCopy.BatchSize, bulkCopy.EnableStreaming,
                                       bulkCopy.DestinationTableName, columns);
      }

      private void LogInserted(SqlBulkCopyOptions options, TimeSpan duration, SqlBulkCopy bulkCopy, string columns)
      {
         _logger.Logger.LogInformation(EventIds.Inserted, @"Executed DbCommand ({duration}ms) [SqlBulkCopyOptions={SqlBulkCopyOptions}, BulkCopyTimeout={BulkCopyTimeout}, BatchSize={BatchSize}, EnableStreaming={EnableStreaming}]
INSERT BULK {table} ({columns})", (long)duration.TotalMilliseconds,
                                       options, bulkCopy.BulkCopyTimeout, bulkCopy.BatchSize, bulkCopy.EnableStreaming,
                                       bulkCopy.DestinationTableName, columns);
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

         if (!(options is SqlServerTempTableBulkInsertOptions sqlServerOptions))
         {
            sqlServerOptions = new SqlServerTempTableBulkInsertOptions(options);
            options = sqlServerOptions;
         }

         var entityType = _ctx.Model.GetEntityType(typeof(T));
         var tempTableCtx = CreateTempTableContext(sqlServerOptions);
         var bulkInsertCtx = CreateBulkInsertContext(options.BulkInsertOptions);

         await _ctx.Database.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);

         try
         {
            var tempTableReference = await CreateTableAndBulkInsertAsync(entityType, entities, tempTableCtx, bulkInsertCtx, cancellationToken).ConfigureAwait(false);
            var query = _ctx.Set<T>().FromSqlRaw($"SELECT * FROM {_sqlGenerationHelper.DelimitIdentifier(tempTableReference.Name)}");

            return new TempTableQuery<T>(query, tempTableReference);
         }
         finally
         {
            await _ctx.Database.CloseConnectionAsync().ConfigureAwait(false);
         }
      }

      private async Task<ITempTableReference> CreateTableAndBulkInsertAsync(
         IEntityType entityType,
         IEnumerable<object> entities,
         TempTableContext tempTableCtx,
         BulkInsertContext bulkInsertCtx,
         CancellationToken cancellationToken)
      {
         var tempTableReference = await tempTableCtx.Creator.CreateTempTableAsync(entityType, tempTableCtx.Options, cancellationToken).ConfigureAwait(false);
         List<ITempTableReference>? ownedTypesTempTableRefs = null;

         try
         {
            await BulkInsertAsync(entityType, entities, null, tempTableReference.Name, bulkInsertCtx, cancellationToken).ConfigureAwait(false);

            if (tempTableCtx.PrimaryKeyCreation == SqlServerPrimaryKeyCreation.AfterBulkInsert)
               await tempTableCtx.Creator.CreatePrimaryKeyAsync(_ctx, entityType, tempTableReference.Name, tempTableCtx.Options.TruncateTableIfExists, cancellationToken).ConfigureAwait(false);

            foreach (var ownedProperty in entityType.GetOwnedTypesProperties(false))
            {
               var ownedEntityType = ownedProperty.GetTargetType();
               var ownedEntities = GetOwnedEntities(entities, ownedProperty);

               var ownedTempTableRef = await CreateTableAndBulkInsertAsync(ownedEntityType, ownedEntities, tempTableCtx, bulkInsertCtx, cancellationToken).ConfigureAwait(false);

               (ownedTypesTempTableRefs ??= new List<ITempTableReference>()).Add(ownedTempTableRef);
            }

            if (ownedTypesTempTableRefs == null)
               return tempTableReference;

            return new OwnerTypeTempTableReference(tempTableReference, ownedTypesTempTableRefs);
         }
         catch (Exception)
         {
            if (ownedTypesTempTableRefs != null)
            {
               foreach (var ownedTypeTempTableRef in ownedTypesTempTableRefs)
               {
                  await ownedTypeTempTableRef.DisposeAsync().ConfigureAwait(false);
               }
            }

            await tempTableReference.DisposeAsync().ConfigureAwait(false);
            throw;
         }
      }

      private TempTableContext CreateTempTableContext(SqlServerTempTableBulkInsertOptions options)
      {
         var tempTableCreator = _ctx.GetService<ISqlServerTempTableCreator>();
         var tempTableOptions = ((ITempTableBulkInsertOptions)options).TempTableCreationOptions;

         if (options.PrimaryKeyCreation == SqlServerPrimaryKeyCreation.AfterBulkInsert && tempTableOptions.CreatePrimaryKey)
            tempTableOptions = new TempTableCreationOptions(tempTableOptions) { CreatePrimaryKey = false };

         return new TempTableContext(tempTableCreator, tempTableOptions, options.PrimaryKeyCreation);
      }

      private BulkInsertContext CreateBulkInsertContext(IBulkInsertOptions options)
      {
         if (!(options is SqlServerBulkInsertOptions sqlServerOptions))
            sqlServerOptions = new SqlServerBulkInsertOptions(options);

         var factory = _ctx.GetService<IEntityDataReaderFactory>();
         var sqlCon = (SqlConnection)_ctx.Database.GetDbConnection();
         var sqlTx = (SqlTransaction?)_ctx.Database.CurrentTransaction?.GetDbTransaction();

         return new BulkInsertContext(factory, sqlCon, sqlTx, sqlServerOptions);
      }

      private class TempTableContext
      {
         public ISqlServerTempTableCreator Creator { get; }
         public ITempTableCreationOptions Options { get; }
         public SqlServerPrimaryKeyCreation PrimaryKeyCreation { get; }

         public TempTableContext(
            ISqlServerTempTableCreator tempTableCreator,
            ITempTableCreationOptions tempTableOptions,
            SqlServerPrimaryKeyCreation primaryKeyCreation)
         {
            Creator = tempTableCreator;
            Options = tempTableOptions;
            PrimaryKeyCreation = primaryKeyCreation;
         }
      }

      private class BulkInsertContext
      {
         public IEntityDataReaderFactory Factory { get; }
         public SqlConnection Connection { get; }
         public SqlTransaction? Transaction { get; }
         public SqlServerBulkInsertOptions Options { get; }

         public BulkInsertContext(
            IEntityDataReaderFactory factory,
            SqlConnection connection,
            SqlTransaction? transaction,
            SqlServerBulkInsertOptions options)
         {
            Factory = factory;
            Connection = connection;
            Transaction = transaction;
            Options = options;
         }
      }
   }
}
