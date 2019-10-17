using System;
using FluentAssertions;
using JetBrains.Annotations;
using Microsoft.EntityFrameworkCore.Storage;
using Xunit;
using Xunit.Abstractions;

namespace Thinktecture.EntityFrameworkCore.Storage.NestedRelationalTransactionManagerTests
{
   public class RollbackTransaction : NestedRelationalTransactionManagerTestBase
   {
      public RollbackTransaction([NotNull] ITestOutputHelper testOutputHelper)
         : base(testOutputHelper)
      {
      }

      [Fact]
      public void Should_throw_InvalidOperationException_no_transaction_active()
      {
         SUT.Invoking(sut => sut.RollbackTransaction())
            .Should().Throw<InvalidOperationException>().WithMessage("The connection does not have any active transactions.");
      }

      [Fact]
      public void Should_commit_root_transaction_and_the_underlying_db_transaction()
      {
         var underlyingTx = SUT.BeginTransaction().GetDbTransaction();

         SUT.RollbackTransaction();

         SUT.CurrentTransaction.Should().BeNull();
         IsTransactionUsable(underlyingTx).Should().BeFalse();
      }

      [Fact]
      public void Should_commit_child_transaction_only()
      {
         var rootTx = SUT.BeginTransaction();
         SUT.BeginTransaction();

         SUT.RollbackTransaction();

         SUT.CurrentTransaction.Should().Be(rootTx);
         IsTransactionUsable(rootTx.GetDbTransaction()).Should().BeTrue();
      }

      [Fact]
      public void Should_create_newest_child_transaction_only()
      {
         var rootTx = SUT.BeginTransaction();
         var childTx = SUT.BeginTransaction();
         var secondChildTx = SUT.BeginTransaction();

         SUT.RollbackTransaction();

         SUT.CurrentTransaction.Should().Be(childTx);
         IsTransactionUsable(childTx.GetDbTransaction()).Should().BeTrue();
      }
   }
}
