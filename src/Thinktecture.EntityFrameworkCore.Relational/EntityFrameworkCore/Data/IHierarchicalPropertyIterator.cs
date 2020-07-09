using System.Collections.Generic;
using Microsoft.EntityFrameworkCore.Metadata;

namespace Thinktecture.EntityFrameworkCore.Data
{
   /// <summary>
   /// For iterations over properties of an entity type and its owned types.
   /// </summary>
   public interface IHierarchicalPropertyIterator
   {
      /// <summary>
      /// Entity type the iterator can iterate over.
      /// </summary>
      IEntityType EntityType { get; }

      /// <summary>
      /// If the <see cref="EntityType"/> is an owned entity then the navigation that lead to this <see cref="EntityType"/>; <c>null</c> otherwise.
      /// </summary>
      INavigation? Navigation { get; }

      /// <summary>
      /// Gets the properties of the <see cref="EntityType"/>.
      /// </summary>
      /// <returns>A collection of properties of <see cref="EntityType"/>.</returns>
      IEnumerable<IProperty> GetProperties();

      /// <summary>
      /// Iterators for the owned types of the <see cref="EntityType"/>.
      /// </summary>
      /// <param name="inlinedOwnTypes">Indication whether inlined (<c>true</c>), separated (<c>false</c>) or all owned types to return.</param>
      /// <returns>A collection of the iterators for the owned types.</returns>
      IEnumerable<IHierarchicalPropertyIterator> GetOwnedTypes(bool? inlinedOwnTypes = null);
   }
}
