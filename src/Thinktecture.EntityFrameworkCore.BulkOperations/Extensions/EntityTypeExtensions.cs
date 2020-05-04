using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;

// ReSharper disable once CheckNamespace
namespace Thinktecture
{
   /// <summary>
   /// Extension methods for <see cref="IEntityType"/>.
   /// </summary>
   public static class EntityTypeExtensions
   {
      /// <summary>
      /// Fetches owned types of the <paramref name="entityType"/>.
      /// </summary>
      /// <param name="entityType">Entity type to fetch owned types for.</param>
      /// <param name="inlinedOwnTypes">Indication whether to fetch inlined owned types, non-inlined or all of them.</param>
      /// <returns>Navigations pointing to owned types.</returns>
      /// <exception cref="ArgumentNullException"></exception>
      public static IEnumerable<INavigation> GetOwnedTypesProperties(
         this IEntityType entityType,
         bool? inlinedOwnTypes = null)
      {
         if (entityType == null)
            throw new ArgumentNullException(nameof(entityType));

         return entityType.GetNavigations()
                          .Where(n => n.ForeignKey.IsOwnership &&
                                      n.ForeignKey.PrincipalEntityType == entityType &&
                                      (inlinedOwnTypes == null || inlinedOwnTypes == IsOwnedTypeInline(entityType, n)));
      }

      /// <summary>
      /// Indication whether the owned type is persisted in the same table as the owner.
      /// </summary>
      /// <param name="entityType">Entity type.</param>
      /// <param name="ownedType">Navigation pointing to the owned type.</param>
      /// <returns><c>true</c> if the owned type is persisted in the same table as the owner; otherwise <c>false</c>.</returns>
      /// <exception cref="ArgumentNullException"></exception>
      public static bool IsOwnedTypeInline(
         this IEntityType entityType,
         INavigation ownedType)
      {
         if (entityType == null)
            throw new ArgumentNullException(nameof(entityType));
         if (ownedType == null)
            throw new ArgumentNullException(nameof(ownedType));

         var targetType = ownedType.GetTargetType();

         return entityType.GetSchema() == targetType.GetSchema() &&
                entityType.GetTableName() == targetType.GetTableName();
      }
   }
}
