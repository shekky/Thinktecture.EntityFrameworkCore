using System.Collections.Generic;
using System.Data;
using Microsoft.EntityFrameworkCore.Metadata;

namespace Thinktecture.EntityFrameworkCore.Data
{
   /// <summary>
   /// Data reader to be used for bulk inserts.
   /// </summary>
   public interface IEntityDataReader : IDataReader
   {
      /// <summary>
      /// Gets the properties that are read by the reader including their index.
      /// </summary>
      /// <returns>A collection of properties.</returns>
      IEnumerable<(int index, IProperty property)> GetProperties();
   }
}
