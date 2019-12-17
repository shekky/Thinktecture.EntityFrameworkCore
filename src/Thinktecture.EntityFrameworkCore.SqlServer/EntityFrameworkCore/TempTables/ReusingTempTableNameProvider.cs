using System;
using System.Collections.Generic;
using System.Linq;
using JetBrains.Annotations;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Thinktecture.EntityFrameworkCore.TempTables.NameSuffixing;

namespace Thinktecture.EntityFrameworkCore.TempTables
{
   /// <summary>
   /// Re-uses the temp table names.
   /// </summary>
   public class ReusingTempTableNameProvider : ITempTableNameProvider
   {
      /// <summary>
      /// An instance of <see cref="ReusingTempTableNameProvider"/>.
      /// </summary>
      public static readonly ITempTableNameProvider Instance = new ReusingTempTableNameProvider();

      /// <inheritdoc />
      public ITempTableNameLease LeaseName(DbContext ctx, IEntityType entityType)
      {
         if (ctx == null)
            throw new ArgumentNullException(nameof(ctx));
         if (entityType == null)
            throw new ArgumentNullException(nameof(entityType));

         var nameLeasing = ctx.GetService<TempTableSuffixLeasing>();
         var suffixLease = nameLeasing.Lease(entityType);

         try
         {
            var tableName = entityType.Relational().TableName;
            tableName = $"{tableName}_{suffixLease.Suffix}";

            return new TempTableNameLease(tableName, suffixLease);
         }
         catch
         {
            suffixLease.Dispose();
            throw;
         }
      }

      private class TempTableNameLease : ITempTableNameLease
      {
         private TempTableSuffixLease _suffixLease;

         public string Name { get; }

         public TempTableNameLease([NotNull] string name, TempTableSuffixLease suffixLease)
         {
            Name = name ?? throw new ArgumentNullException(nameof(name));
            _suffixLease = suffixLease;
         }

         public void Dispose()
         {
            _suffixLease.Dispose();
            _suffixLease = default;
         }
      }
   }
}
