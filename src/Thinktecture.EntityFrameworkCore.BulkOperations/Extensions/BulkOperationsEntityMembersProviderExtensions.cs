using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Thinktecture.EntityFrameworkCore;
using Thinktecture.EntityFrameworkCore.BulkOperations;
using Thinktecture.EntityFrameworkCore.Data;

// ReSharper disable once CheckNamespace
namespace Thinktecture
{
   /// <summary>
   /// Extensions for <see cref="IEntityMembersProvider"/>.
   /// </summary>
   public static class BulkOperationsEntityMembersProviderExtensions
   {
      /// <summary>
      /// Determines properties to include into a temp table into.
      /// </summary>
      /// <param name="entityMembersProvider">Entity member provider.</param>
      /// <param name="entityType">Entity type.</param>
      /// <returns>Properties to include into a temp table.</returns>
      public static IHierarchicalPropertyIterator GetPropertiesForTempTable(
         this IEntityMembersProvider? entityMembersProvider,
         IEntityType entityType)
      {
         if (entityMembersProvider == null)
            return new EntityTypeBasedHierarchicalPropertyIterator(entityType, null, PropertiesForTempTableFilter);

         return new HierarchicalPropertyIterator(entityType, null, entityMembersProvider.GetMembers(), PropertiesForTempTableFilter);
      }

      /// <summary>
      /// Determines properties to insert into a (temp) table.
      /// </summary>
      /// <param name="entityMembersProvider">Entity member provider.</param>
      /// <param name="entityType">Entity type.</param>
      /// <returns>Properties to use insert into a (temp) table.</returns>
      public static IHierarchicalPropertyIterator GetPropertiesForInsert(
         this IEntityMembersProvider? entityMembersProvider,
         IEntityType entityType)
      {
         if (entityMembersProvider == null)
            return new EntityTypeBasedHierarchicalPropertyIterator(entityType, null, PropertiesForInsertFilter);

         return new HierarchicalPropertyIterator(entityType, null, entityMembersProvider.GetMembers(), PropertiesForInsertFilter);
      }

      private static bool PropertiesForTempTableFilter(
         IProperty property,
         INavigation? navigation)
      {
         return navigation == null || !property.IsKey();
      }

      private static bool PropertiesForInsertFilter(
         IProperty property,
         INavigation? navigation)
      {
         return property.GetBeforeSaveBehavior() != PropertySaveBehavior.Ignore &&
                (navigation == null || !property.IsKey());
      }
   }
}
