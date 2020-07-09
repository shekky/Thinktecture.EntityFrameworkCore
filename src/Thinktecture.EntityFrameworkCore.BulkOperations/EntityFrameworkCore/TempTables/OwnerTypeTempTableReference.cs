using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Thinktecture.EntityFrameworkCore.TempTables
{
   /// <summary>
   /// Temp table reference for owned entities.
   /// </summary>
   public sealed class OwnerTypeTempTableReference : ITempTableReference
   {
      private readonly ITempTableReference _ownerTempTableReference;
      private readonly IReadOnlyList<ITempTableReference> _ownedTempTableRefs;

      /// <inheritdoc />
      public string Name => _ownerTempTableReference.Name;

      /// <summary>
      /// Initializes new instance of <see cref="OwnerTypeTempTableReference"/>.
      /// </summary>
      /// <param name="ownerTempTableReference"><see cref="ITempTableReference"/> of the owner.</param>
      /// <param name="ownedTempTableRefs"><see cref="ITempTableReference"/> of the owned entities.</param>
      public OwnerTypeTempTableReference(
         ITempTableReference ownerTempTableReference,
         IReadOnlyList<ITempTableReference> ownedTempTableRefs)
      {
         _ownerTempTableReference = ownerTempTableReference;
         _ownedTempTableRefs = ownedTempTableRefs ?? throw new ArgumentNullException(nameof(ownedTempTableRefs));
      }

      /// <inheritdoc />
      public async ValueTask DisposeAsync()
      {
         foreach (var tableRef in _ownedTempTableRefs)
         {
            await tableRef.DisposeAsync().ConfigureAwait(false);
         }

         await _ownerTempTableReference.DisposeAsync().ConfigureAwait(false);
      }

      /// <inheritdoc />
      public void Dispose()
      {
         foreach (var tableRef in _ownedTempTableRefs)
         {
            tableRef.Dispose();
         }

         _ownerTempTableReference.Dispose();
      }
   }
}
