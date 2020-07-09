using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;

namespace Thinktecture.EntityFrameworkCore.Data
{
   internal class EntityTypeBasedHierarchicalPropertyIterator : IHierarchicalPropertyIterator
   {
      private readonly Func<IProperty, INavigation?, bool> _filter;

      public IEntityType EntityType { get; }
      public INavigation? Navigation { get; }

      public EntityTypeBasedHierarchicalPropertyIterator(
         IEntityType entityType,
         INavigation? navigation,
         Func<IProperty, INavigation?, bool> filter)
      {
         EntityType = entityType ?? throw new ArgumentNullException(nameof(entityType));
         Navigation = navigation;
         _filter = filter ?? throw new ArgumentNullException(nameof(filter));
      }

      public IEnumerable<IProperty> GetProperties()
      {
         return EntityType.GetProperties().Where(p => _filter(p, Navigation));
      }

      public IEnumerable<IHierarchicalPropertyIterator> GetOwnedTypes(
         bool? inlinedOwnTypes = null)
      {
         return EntityType.GetOwnedTypesProperties(inlinedOwnTypes)
                          .Select(navi => (IHierarchicalPropertyIterator)new EntityTypeBasedHierarchicalPropertyIterator(navi.GetTargetType(), navi, _filter));
      }
   }
}
