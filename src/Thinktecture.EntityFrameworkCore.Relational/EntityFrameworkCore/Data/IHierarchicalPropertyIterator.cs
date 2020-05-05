using System.Collections.Generic;
using Microsoft.EntityFrameworkCore.Metadata;

namespace Thinktecture.EntityFrameworkCore.Data
{
   /// <summary>
   /// For iterations over properties of an entity type and its owned types.
   /// </summary>
   public interface IHierarchicalPropertyIterator
   {
      IEntityType EntityType { get; }
      INavigation? Navigation { get; }
      IEnumerable<IProperty> GetProperties();
      IEnumerable<IHierarchicalPropertyIterator> GetOwnedTypes(bool? inlinedOwnTypes = null);
   }
}
