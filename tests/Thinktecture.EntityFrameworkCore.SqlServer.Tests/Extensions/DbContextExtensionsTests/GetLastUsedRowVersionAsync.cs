using System.Threading;
using System.Threading.Tasks;
using FluentAssertions;
using JetBrains.Annotations;
using Xunit;
using Xunit.Abstractions;

namespace Thinktecture.Extensions.DbContextExtensionsTests
{
   // ReSharper disable once InconsistentNaming
   public class GetLastUsedRowVersionAsync : IntegrationTestsBase
   {
      public GetLastUsedRowVersionAsync([NotNull] ITestOutputHelper testOutputHelper)
         : base(testOutputHelper, true)
      {
      }

      [Fact]
      public async Task Should_fetch_last_used_rowversion()
      {
         var rowVersion = await ActDbContext.GetLastUsedRowVersionAsync(CancellationToken.None).ConfigureAwait(false);
         rowVersion.Should().NotBe(0);
      }
   }
}
