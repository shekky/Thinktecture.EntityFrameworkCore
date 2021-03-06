using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using JetBrains.Annotations;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Internal;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Migrations.Operations;
using Microsoft.Extensions.DependencyInjection;

namespace Thinktecture.EntityFrameworkCore.Migrations
{
   /// <summary>
   /// An implementation of <see cref="IMigrationsAssembly"/> that is able to instantiate migrations requiring an <see cref="IDbDefaultSchema"/>.
   /// </summary>
   public class DefaultSchemaRespectingMigrationAssembly<TMigrationsAssembly> : IMigrationsAssembly
      where TMigrationsAssembly : class, IMigrationsAssembly
   {
      private readonly TMigrationsAssembly _innerMigrationsAssembly;
      private readonly IMigrationOperationSchemaSetter _schemaSetter;
      private readonly IServiceProvider _serviceProvider;
      private readonly DbContext _context;

      /// <inheritdoc />
      public IReadOnlyDictionary<string, TypeInfo> Migrations => _innerMigrationsAssembly.Migrations;

      /// <inheritdoc />
      public ModelSnapshot ModelSnapshot => _innerMigrationsAssembly.ModelSnapshot;

      /// <inheritdoc />
      public Assembly Assembly => _innerMigrationsAssembly.Assembly;

      /// <inheritdoc />
      public DefaultSchemaRespectingMigrationAssembly([NotNull] TMigrationsAssembly migrationsAssembly,
                                                      [NotNull] IMigrationOperationSchemaSetter schemaSetter,
                                                      [NotNull] ICurrentDbContext currentContext,
                                                      [NotNull] IServiceProvider serviceProvider)
      {
         _innerMigrationsAssembly = migrationsAssembly ?? throw new ArgumentNullException(nameof(migrationsAssembly));
         _schemaSetter = schemaSetter ?? throw new ArgumentNullException(nameof(schemaSetter));
         _serviceProvider = serviceProvider ?? throw new ArgumentNullException(nameof(serviceProvider));
         // ReSharper disable once ConstantConditionalAccessQualifier
         _context = currentContext?.Context ?? throw new ArgumentNullException(nameof(currentContext));
      }

      /// <inheritdoc />
      public string FindMigrationId(string nameOrId)
      {
         return _innerMigrationsAssembly.FindMigrationId(nameOrId);
      }

      /// <inheritdoc />
      [NotNull]
      public Migration CreateMigration(TypeInfo migrationClass, string activeProvider)
      {
         if (migrationClass == null)
            throw new ArgumentNullException(nameof(migrationClass));
         if (activeProvider == null)
            throw new ArgumentNullException(nameof(activeProvider));

         var hasCtorWithDefaultSchema = migrationClass.GetConstructors().Any(c => c.GetParameters().Any(p => p.ParameterType == typeof(IDbDefaultSchema)));

         // has default schema
         if (_context is IDbDefaultSchema schema)
         {
            var migration = hasCtorWithDefaultSchema ? CreateInstance(migrationClass, schema, activeProvider) : CreateInstance(migrationClass, activeProvider);

            SetSchema(migration.UpOperations, schema);
            SetSchema(migration.DownOperations, schema);

            return migration;
         }

         if (!hasCtorWithDefaultSchema)
            return CreateInstance(migrationClass, activeProvider);

         throw new ArgumentException($"For instantiation of default schema respecting migration of type '{migrationClass.Name}' the database context of type '{_context.GetType().DisplayName()}' has to implement the interface '{nameof(IDbDefaultSchema)}'.", nameof(migrationClass));
      }

      [NotNull]
      private Migration CreateInstance([NotNull] TypeInfo migrationClass, IDbDefaultSchema schema, string activeProvider)
      {
         var migration = (Migration)ActivatorUtilities.CreateInstance(_serviceProvider, migrationClass.AsType(), schema);
         migration.ActiveProvider = activeProvider;

         return migration;
      }

      [NotNull]
      private Migration CreateInstance([NotNull] TypeInfo migrationClass, string activeProvider)
      {
         var migration = (Migration)ActivatorUtilities.CreateInstance(_serviceProvider, migrationClass.AsType());
         migration.ActiveProvider = activeProvider;

         return migration;
      }

      private void SetSchema([NotNull] IReadOnlyList<MigrationOperation> operations, [CanBeNull] IDbDefaultSchema schema)
      {
         if (schema?.Schema != null)
            _schemaSetter.SetSchema(operations, schema.Schema);
      }
   }
}
