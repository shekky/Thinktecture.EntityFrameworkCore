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
      IEnumerable<(int index, IProperty property)> GetProperties();
   }
}
