using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.EntityFrameworkCore.Metadata;
using Thinktecture.EntityFrameworkCore.Data;

// ReSharper disable once CheckNamespace
namespace Thinktecture
{
   /// <summary>
   /// Extension methods for <see cref="IHierarchicalPropertyIterator"/>
   /// </summary>
   public static class HierarchicalPropertyIteratorExtensions
   {
      /// <summary>
      /// Iterates over the provided hierarchical <paramref name="iterator"/> and collects properties from all levels.
      /// </summary>
      /// <param name="iterator">Iterator to collect properties from.</param>
      /// <param name="inlinedOwnTypes">Indication whether inlined (<c>true</c>), separated (<c>false</c>) or all owned types to return.</param>
      /// <returns>Collected properties.</returns>
      /// <exception cref="ArgumentNullException"><paramref name="iterator"/> is <c>null</c>.</exception>
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
