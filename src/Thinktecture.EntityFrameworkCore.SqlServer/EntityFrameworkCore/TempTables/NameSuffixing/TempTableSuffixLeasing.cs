using System;
using System.Collections.Generic;
using System.Data.Common;
using JetBrains.Annotations;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Internal;
using Microsoft.EntityFrameworkCore.Metadata;

namespace Thinktecture.EntityFrameworkCore.TempTables.NameSuffixing
{
#pragma warning disable CA1812

   internal class TempTableSuffixLeasing : IDisposable
   {
      private readonly object _lock;
      private readonly TempTableSuffixCache _cache;
      private readonly ICurrentDbContext _currentDbContext;

      private DbConnection _connection;
      private Dictionary<IEntityType, TempTableSuffixes> _lookup;
      private bool _idDisposed;

      public TempTableSuffixLeasing(
         [NotNull] ICurrentDbContext currentDbContext,
         [NotNull] TempTableSuffixCache cache)
      {
         _cache = cache ?? throw new ArgumentNullException(nameof(cache));
         _currentDbContext = currentDbContext ?? throw new ArgumentNullException(nameof(currentDbContext));
         _lock = new object();

         // don't fetch the connection and suffix lookup immediately but on first use only
      }

      public TempTableSuffixLease Lease([NotNull] IEntityType entityType)
      {
         if (entityType == null)
            throw new ArgumentNullException(nameof(entityType));

         EnsureDisposed();

         if (_lookup == null)
         {
            lock (_lock)
            {
               EnsureDisposed();

               if (_lookup == null)
               {
                  _connection = _currentDbContext.Context.Database.GetDbConnection();
                  _lookup = _cache.LeaseSuffixLookup(_connection);
               }
            }
         }

         if (!_lookup.TryGetValue(entityType, out var suffixes))
         {
            suffixes = new TempTableSuffixes();
            _lookup.Add(entityType, suffixes);
         }

         return suffixes.Lease();
      }

      private void EnsureDisposed()
      {
         if (_idDisposed)
            throw new ObjectDisposedException(nameof(TempTableSuffixLease));
      }

      public void Dispose()
      {
         if (_idDisposed)
            return;

         _idDisposed = true;

         lock (_lock)
         {
            if (_connection == null)
               return;

            _cache.ReturnSuffixLookup(_connection);
            _lookup = null;
            _connection = null;
         }
      }
   }
}
