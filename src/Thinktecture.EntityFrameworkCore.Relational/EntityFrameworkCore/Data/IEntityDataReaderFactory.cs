using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.Extensions.Logging;

namespace Thinktecture.EntityFrameworkCore.Data
{
   /// <summary>
   /// Factory for creation of <see cref="IEntityDataReader"/>.
   /// </summary>
   public interface IEntityDataReaderFactory
   {
      /// <summary>
      /// Creates an <see cref="IEntityDataReader"/> for entities of type <typeparamref name="T"/>.
      /// The data reader reads the provided <paramref name="properties"/> only.
      /// </summary>
      /// <param name="ctx">Database context.</param>
      /// <param name="entities">Entities to use by the data reader.</param>
      /// <param name="properties">Properties of the entity of type <typeparamref name="T"/> to generate the data reader for.</param>
      /// <typeparam name="T">Type of the entity.</typeparam>
      /// <returns>An instance of <see cref="IEntityDataReader"/>.</returns>
      IEntityDataReader Create<T>(
         DbContext ctx,
         IEnumerable<T> entities,
         IHierarchicalPropertyIterator properties)
         where T : class;
   }
}
