using System;
using System.Data.Common;
using JetBrains.Annotations;
using Microsoft.EntityFrameworkCore.Diagnostics;

namespace Thinktecture.EntityFrameworkCore.Storage
{
   /// <summary>
   /// A child transaction.
   /// </summary>
   public class ChildNestedDbContextTransaction : NestedDbContextTransaction
   {
      private readonly NestedDbContextTransaction _parent;

      /// <summary>
      /// Initializes new instance of <see cref="ChildNestedDbContextTransaction"/>.
      /// </summary>
      /// <param name="logger">Logger.</param>
      /// <param name="nestedTransactionManager">Nested transaction manager.</param>
      /// <param name="parent">Parent transaction.</param>
      public ChildNestedDbContextTransaction([NotNull] IDiagnosticsLogger<RelationalDbLoggerCategory.NestedTransaction> logger,
                                             [NotNull] NestedRelationalTransactionManager nestedTransactionManager,
                                             [NotNull] NestedDbContextTransaction parent)
         : base(logger, nestedTransactionManager, Guid.NewGuid())
      {
         _parent = parent ?? throw new ArgumentNullException(nameof(parent));
      }

      /// <inheritdoc />
      protected internal override DbTransaction GetUnderlyingTransaction()
      {
         return _parent.GetUnderlyingTransaction();
      }
   }
}
