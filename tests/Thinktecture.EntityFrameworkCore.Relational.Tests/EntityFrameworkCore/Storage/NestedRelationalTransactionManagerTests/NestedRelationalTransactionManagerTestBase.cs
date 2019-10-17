using System;
using System.Data.Common;
using JetBrains.Annotations;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Xunit.Abstractions;

namespace Thinktecture.EntityFrameworkCore.Storage.NestedRelationalTransactionManagerTests
{
   public abstract class NestedRelationalTransactionManagerTestBase : IntegrationTestsBase
   {
      protected NestedRelationalTransactionManager SUT => ActDbContext.GetService<NestedRelationalTransactionManager>();

      protected NestedRelationalTransactionManagerTestBase([NotNull] ITestOutputHelper testOutputHelper,
                                                           [CanBeNull] IMigrationExecutionStrategy migrationExecutionStrategy = null)
         : base(testOutputHelper, migrationExecutionStrategy ?? MigrationExecutionStrategies.NoMigration)
      {
         ConfigureOptionsBuilder = builder => builder.AddNestedTransactionSupport();
      }

      protected bool IsTransactionUsable(DbTransaction tx)
      {
         try
         {
            var connection = ActDbContext.Database.GetDbConnection();
            var command = connection.CreateCommand();
            command.Transaction = tx;
            command.CommandText = "PRAGMA user_version;";
            command.ExecuteNonQuery();
         }
         catch (InvalidOperationException ex)
         {
            if (ex.Message == "The transaction object is not associated with the connection object.")
               return false;
         }

         return true;
      }
   }
}
