using System;
using JetBrains.Annotations;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Query.ExpressionTranslators;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Thinktecture.EntityFrameworkCore.BulkOperations;
using Thinktecture.EntityFrameworkCore.Data;
using Thinktecture.EntityFrameworkCore.Migrations;
using Thinktecture.EntityFrameworkCore.Query.ExpressionTranslators;
using Thinktecture.EntityFrameworkCore.TempTables;
using Thinktecture.EntityFrameworkCore.TempTables.NameSuffixing;

namespace Thinktecture.EntityFrameworkCore.Infrastructure
{
   /// <summary>
   /// Extensions for DbContextOptions.
   /// </summary>
   public class SqlServerDbContextOptionsExtension : IDbContextOptionsExtension
   {
      /// <inheritdoc />
      [NotNull]
      public string LogFragment => $@"
{{
   'RowNumberSupport'={AddRowNumberSupport},
   'BulkOperationSupport'={AddBulkOperationSupport},
   'TempTableSupport'={AddTempTableSupport},
   'UseThinktectureSqlServerMigrationsSqlGenerator'={UseThinktectureSqlServerMigrationsSqlGenerator}
}}";

      /// <summary>
      /// Enables and disables support for "RowNumber".
      /// </summary>
      public bool AddRowNumberSupport { get; set; }

      /// <summary>
      /// Enables and disables support for temp tables.
      /// </summary>
      public bool AddTempTableSupport { get; set; }

      private bool _addBulkOperationSupport;

      /// <summary>
      /// Enables and disables support for bulk operations.
      /// </summary>
      public bool AddBulkOperationSupport
      {
         get => _addBulkOperationSupport || AddTempTableSupport; // temp tables require bulk operations
         set => _addBulkOperationSupport = value;
      }

      /// <summary>
      /// Changes the implementation of <see cref="IMigrationsSqlGenerator"/> to <see cref="ThinktectureSqlServerMigrationsSqlGenerator"/>.
      /// </summary>
      public bool UseThinktectureSqlServerMigrationsSqlGenerator { get; set; }

      /// <inheritdoc />
      public bool ApplyServices(IServiceCollection services)
      {
         services.TryAddSingleton(this);
         services.Add<IMethodCallTranslatorPlugin, SqlServerMethodCallTranslatorPlugin>(GetLifetime<IMethodCallTranslatorPlugin>());

         if (AddTempTableSupport)
         {
            services.TryAdd<ITempTableCreator, SqlServerTempTableCreator>(GetLifetime<ISqlGenerationHelper>());
            services.AddScoped<TempTableSuffixLeasing>();
         }

         if (AddBulkOperationSupport)
         {
            services.TryAddSingleton<IEntityDataReaderFactory, EntityDataReaderFactory>();
            services.TryAdd<IBulkOperationExecutor, SqlServerBulkOperationExecutor>(GetLifetime<ISqlGenerationHelper>());
         }

         if (UseThinktectureSqlServerMigrationsSqlGenerator)
            services.Add<IMigrationsSqlGenerator, ThinktectureSqlServerMigrationsSqlGenerator>(GetLifetime<IMigrationsSqlGenerator>());

         return false;
      }

      private static ServiceLifetime GetLifetime<TService>()
      {
         return EntityFrameworkRelationalServicesBuilder.RelationalServices[typeof(TService)].Lifetime;
      }

      /// <inheritdoc />
#pragma warning disable CA1024
      public long GetServiceProviderHashCode()
#pragma warning restore CA1024
      {
         return 0;
      }

      /// <inheritdoc />
      public void Validate(IDbContextOptions options)
      {
      }
   }
}
