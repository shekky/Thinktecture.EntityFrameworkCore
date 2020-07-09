using System;
using System.Collections.Generic;
using System.Reflection;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;

namespace Thinktecture.EntityFrameworkCore.Data
{
   internal class HierarchicalPropertyIterator : IHierarchicalPropertyIterator
   {
      private readonly IReadOnlyList<MemberInfo> _members;
      private readonly Func<IProperty, INavigation?, bool> _filter;

      private IEnumerable<IProperty>? _properties;
      private IEnumerable<IHierarchicalPropertyIterator>? _ownedProperties;

      public IEntityType EntityType { get; }
      public INavigation? Navigation { get; }

      public HierarchicalPropertyIterator(
         IEntityType entityType,
         INavigation? navigation,
         IReadOnlyList<MemberInfo> members,
         Func<IProperty, INavigation?, bool> filter)
      {
         EntityType = entityType ?? throw new ArgumentNullException(nameof(entityType));
         Navigation = navigation;
         _members = members ?? throw new ArgumentNullException(nameof(members));
         _filter = filter ?? throw new ArgumentNullException(nameof(filter));
      }

      public IEnumerable<IProperty> GetProperties()
      {
         if (_properties == null)
            Initialize();

         return _properties!;
      }

      public IEnumerable<IHierarchicalPropertyIterator> GetOwnedTypes(bool? inlinedOwnTypes = null)
      {
         if (_ownedProperties == null)
            Initialize();

         return _ownedProperties!;
      }

      private void Initialize()
      {
         var properties = new List<IProperty>();
         var ownedProperties = new List<IHierarchicalPropertyIterator>();

         foreach (var memberInfo in _members)
         {
            var property = FindProperty(memberInfo);

            if (property != null)
            {
               if (!_filter(property, Navigation))
                  throw new InvalidOperationException($"The member '{memberInfo.Name}' of the entity '{EntityType.Name}' cannot be written to database.");

               properties.Add(property);
            }
            else
            {
               var ownedNavi = FindOwnedProperty(memberInfo);

               if (ownedNavi == null)
                  throw new Exception($"The member '{memberInfo.Name}' has not been found on entity '{EntityType.Name}'.");

               ownedProperties.Add(new EntityTypeBasedHierarchicalPropertyIterator(ownedNavi.GetTargetType(), ownedNavi, _filter));
            }
         }

         _properties = properties;
         _ownedProperties = ownedProperties;
      }

      private IProperty? FindProperty(MemberInfo memberInfo)
      {
         foreach (var property in EntityType.GetProperties())
         {
            if (property.PropertyInfo == memberInfo || property.FieldInfo == memberInfo)
               return property;
         }

         return null;
      }

      private INavigation? FindOwnedProperty(MemberInfo memberInfo)
      {
         foreach (var ownedTypeNavi in EntityType.GetOwnedTypesProperties())
         {
            if (ownedTypeNavi.PropertyInfo == memberInfo || ownedTypeNavi.FieldInfo == memberInfo)
            {
               if (EntityType.IsOwnedTypeInline(ownedTypeNavi))
                  return ownedTypeNavi;

               throw new NotSupportedException($"Properties of owned types that are saved in a separate table are not supported. Property: {ownedTypeNavi.Name}");
            }
         }

         return null;
      }
   }
}
