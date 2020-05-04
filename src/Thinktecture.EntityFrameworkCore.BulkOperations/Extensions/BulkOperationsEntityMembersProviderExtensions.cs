using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Thinktecture.EntityFrameworkCore.BulkOperations;

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
      public static IReadOnlyList<IProperty> GetPropertiesForTempTable(
         this IEntityMembersProvider? entityMembersProvider,
         IEntityType entityType)
      {
         if (entityType == null)
            throw new ArgumentNullException(nameof(entityType));

         if (entityMembersProvider == null)
         {
            var properties = entityType.GetProperties().ToList();
            AddInlineOwnedTypes(properties, entityType);

            return properties;
         }

         return ConvertToEntityProperties(entityMembersProvider.GetMembers(), entityType);
      }

      private static void AddInlineOwnedTypes(
         List<IProperty> properties,
         IEntityType entityType)
      {
         foreach (var ownedNavi in entityType.GetOwnedTypesProperties(true))
         {
            AddOwnedTypeProperties(properties, ownedNavi);
         }
      }

      private static void AddOwnedTypeProperties(List<IProperty> properties, INavigation ownedNavi)
      {
         var ownedType = ownedNavi.GetTargetType();
         var ownedProps = ownedType.GetProperties();

         properties.AddRange(ownedProps.Where(p => !p.IsKey()));
      }

      /// <summary>
      /// Determines properties to insert into a (temp) table.
      /// </summary>
      /// <param name="entityMembersProvider">Entity member provider.</param>
      /// <param name="entityType">Entity type.</param>
      /// <returns>Properties to use insert into a (temp) table.</returns>
      public static IReadOnlyList<IProperty> GetPropertiesForInsert(
         this IEntityMembersProvider? entityMembersProvider,
         IEntityType entityType)
      {
         if (entityType == null)
            throw new ArgumentNullException(nameof(entityType));

         if (entityMembersProvider == null)
            return entityType.GetProperties().Where(p => p.GetBeforeSaveBehavior() != PropertySaveBehavior.Ignore).ToList();

         return ConvertToEntityProperties(entityMembersProvider.GetMembers(), entityType);
      }

      private static IReadOnlyList<IProperty> ConvertToEntityProperties(IReadOnlyList<MemberInfo> memberInfos, IEntityType entityType)
      {
         var properties = new List<IProperty>();

         for (var i = 0; i < memberInfos.Count; i++)
         {
            var memberInfo = memberInfos[i];
            var property = FindProperty(entityType, memberInfo);

            if (property != null)
            {
               properties.Add(property);
            }
            else
            {
               var ownedNavi = FindOwnedProperty(entityType, memberInfo);

               if (ownedNavi == null)
                  throw new ArgumentException($"The member '{memberInfo.Name}' has not been found on entity '{entityType.Name}'.", nameof(memberInfos));

               AddOwnedTypeProperties(properties, ownedNavi);
            }
         }

         return properties;
      }

      private static IProperty? FindProperty(IEntityType entityType, MemberInfo memberInfo)
      {
         foreach (var property in entityType.GetProperties())
         {
            if (property.PropertyInfo == memberInfo || property.FieldInfo == memberInfo)
               return property;
         }

         return null;
      }

      private static INavigation? FindOwnedProperty(IEntityType entityType, MemberInfo memberInfo)
      {
         foreach (var ownedTypeNavi in entityType.GetOwnedTypesProperties())
         {
            if (ownedTypeNavi.PropertyInfo == memberInfo || ownedTypeNavi.FieldInfo == memberInfo)
            {
               if (entityType.IsOwnedTypeInline(ownedTypeNavi))
                  return ownedTypeNavi;

               throw new NotSupportedException("Properties of owned types that are saved in a separate table are not supported.");
            }
         }

         return null;
      }
   }
}
