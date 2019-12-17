using System;
using System.Data;
using JetBrains.Annotations;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.Logging;

namespace Thinktecture.EntityFrameworkCore.TempTables
{
   /// <summary>
   /// A reference to SQL Server temp table.
   /// </summary>
   public sealed class SqlServerTempTableReference : ITempTableReference
   {
      private readonly IDiagnosticsLogger<DbLoggerCategory.Query> _logger;
      private readonly ISqlGenerationHelper _sqlGenerationHelper;
      private readonly DatabaseFacade _database;
      private readonly ITempTableNameLease _nameLease;
      private readonly bool _dropTableOnDispose;

      /// <inheritdoc />
      public string Name { get; }

      /// <summary>
      /// Initializes new instance of <see cref="SqlServerTempTableReference"/>.
      /// </summary>
      /// <param name="logger">Logger</param>
      /// <param name="sqlGenerationHelper">SQL generation helper.</param>
      /// <param name="tableName">The name of the temp table.</param>
      /// <param name="database">Database facade.</param>
      /// <param name="nameLease">Leased table name that will be disposed along with the temp table.</param>
      /// <param name="dropTableOnDispose">Indication whether to drop the temp table on dispose or not.</param>
      public SqlServerTempTableReference(
         [NotNull] IDiagnosticsLogger<DbLoggerCategory.Query> logger,
         [NotNull] ISqlGenerationHelper sqlGenerationHelper,
         [NotNull] string tableName,
         [NotNull] DatabaseFacade database,
         [NotNull] ITempTableNameLease nameLease,
         bool dropTableOnDispose)
      {
         Name = tableName ?? throw new ArgumentNullException(nameof(tableName));
         _logger = logger ?? throw new ArgumentNullException(nameof(logger));
         _sqlGenerationHelper = sqlGenerationHelper ?? throw new ArgumentNullException(nameof(sqlGenerationHelper));
         _database = database ?? throw new ArgumentNullException(nameof(database));
         _nameLease = nameLease ?? throw new ArgumentNullException(nameof(nameLease));
         _dropTableOnDispose = dropTableOnDispose;
      }

      /// <inheritdoc />
      public void Dispose()
      {
         try
         {
            if (!_dropTableOnDispose || _database.GetDbConnection().State != ConnectionState.Open)
               return;

            // ReSharper disable once RedundantCast because the "name" should not be sent as a parameter.
            _database.ExecuteSqlCommand((string)$"DROP TABLE IF EXISTS {_sqlGenerationHelper.DelimitIdentifier(Name)}");
            _database.CloseConnection();
         }
         catch (ObjectDisposedException ex)
         {
            _logger.Logger.LogWarning(ex, $"Trying to dispose of the temp table reference '{Name}' after the corresponding DbContext has been disposed.");
         }
         finally
         {
            _nameLease.Dispose();
         }
      }
   }
}
