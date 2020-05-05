using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.EntityFrameworkCore.Metadata;
using Thinktecture.EntityFrameworkCore.Data;

// ReSharper disable once CheckNamespace
namespace Thinktecture
{
   public static class HierarchicalPropertyIteratorExtensions
   {
      public static IReadOnlyList<IProperty> FlattenProperties(
         this IHierarchicalPropertyIterator iterator,
         bool? inlinedOwnTypes)
      {
         if (iterator == null)
            throw new ArgumentNullException(nameof(iterator));

         var properties = iterator.GetProperties().ToList();

         AddOwnedTypeProperties(properties, iterator, inlinedOwnTypes);

         return properties;
      }

      private static void AddOwnedTypeProperties(
         List<IProperty> properties,
         IHierarchicalPropertyIterator iterator,
         bool? inlinedOwnTypes)
      {
         foreach (var ownedType in iterator.GetOwnedTypes(inlinedOwnTypes))
         {
            properties.AddRange(ownedType.GetProperties());

            AddOwnedTypeProperties(properties, ownedType, inlinedOwnTypes);
         }
      }
   }
}
