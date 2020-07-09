using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;
using FluentAssertions;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Thinktecture.TestDatabaseContext;
using Xunit;
using Xunit.Abstractions;

namespace Thinktecture.Extensions.DbContextExtensionsTests
{
   // ReSharper disable once InconsistentNaming
   [Collection("BulkInsertTempTableAsync")]
   public class BulkInsertIntoTempTableAsync : IntegrationTestsBase
   {
      public BulkInsertIntoTempTableAsync(ITestOutputHelper testOutputHelper)
         : base(testOutputHelper, true)
      {
      }

      [Fact]
      public async Task Should_insert_queryType()
      {
         ConfigureModel = builder => builder.ConfigureTempTableEntity<CustomTempTable>().Property(t => t.Column2).HasMaxLength(100).IsRequired();

         var entities = new List<CustomTempTable> { new CustomTempTable(1, "value") };
         await using var query = await ActDbContext.BulkInsertIntoTempTableAsync(entities);

         var tempTable = await query.Query.ToListAsync();
         tempTable.Should()
                  .HaveCount(1).And
                  .BeEquivalentTo(new CustomTempTable(1, "value"));
      }

      [Fact]
      public async Task Should_insert_entityType_without_touching_real_table()
      {
         var entity = new TestEntity
                      {
                         Id = new Guid("577BFD36-21BC-4F9E-97B4-367B8F29B730"),
                         Name = "Name",
                         Count = 42,
                         ConvertibleClass = new ConvertibleClass(43)
                      };
         ArrangeDbContext.TestEntities.Add(entity);
         await ArrangeDbContext.SaveChangesAsync();

         var entities = new List<TestEntity> { entity };
         await using var query = await ActDbContext.BulkInsertIntoTempTableAsync(entities);

         var tempTable = await query.Query.ToListAsync();
         tempTable.Should()
                  .HaveCount(1).And
                  .BeEquivalentTo(new TestEntity
                                  {
                                     Id = new Guid("577BFD36-21BC-4F9E-97B4-367B8F29B730"),
                                     Name = "Name",
                                     Count = 42,
                                     ConvertibleClass = new ConvertibleClass(43)
                                  });
      }

      [Fact]
      public async Task Should_return_disposable_query()
      {
         await using var tempTableQuery = await ActDbContext.BulkInsertIntoTempTableAsync(Array.Empty<TestEntity>());
         tempTableQuery.Dispose();

         tempTableQuery.Awaiting(t => t.Query.ToListAsync())
                       .Should().Throw<SqlException>().Where(ex => ex.Message.StartsWith("Invalid object name '#TestEntities", StringComparison.Ordinal));
      }

      [Fact]
      public async Task Should_ignore_inlined_owned_type_if_it_is_null()
      {
         var testEntity = new TestEntityOwningInlineEntity
                          {
                             Id = new Guid("3A1B2FFF-8E11-44E5-80E5-8C7FEEDACEB3"),
                             InlineEntity = null!
                          };
         var testEntities = new[] { testEntity };

         await using var tempTableQuery = await ActDbContext.BulkInsertIntoTempTableAsync(testEntities);

         var loadedEntities = await tempTableQuery.Query.ToListAsync();
         loadedEntities.Should().HaveCount(1);
         var loadedEntity = loadedEntities[0];
         loadedEntity.Should().BeEquivalentTo(new TestEntityOwningInlineEntity
                                              {
                                                 Id = new Guid("3A1B2FFF-8E11-44E5-80E5-8C7FEEDACEB3"),
                                                 InlineEntity = null!
                                              });
      }

      [Fact]
      public async Task Should_insert_inlined_owned_type_if_it_has_default_values_only()
      {
         var testEntity = new TestEntityOwningInlineEntity
                          {
                             Id = new Guid("3A1B2FFF-8E11-44E5-80E5-8C7FEEDACEB3"),
                             InlineEntity = new OwnedInlineEntity()
                          };
         var testEntities = new[] { testEntity };

         await using var tempTableQuery = await ActDbContext.BulkInsertIntoTempTableAsync(testEntities);

         var loadedEntities = await tempTableQuery.Query.ToListAsync();
         loadedEntities.Should().HaveCount(1);
         var loadedEntity = loadedEntities[0];
         loadedEntity.Should().BeEquivalentTo(new TestEntityOwningInlineEntity
                                              {
                                                 Id = new Guid("3A1B2FFF-8E11-44E5-80E5-8C7FEEDACEB3"),
                                                 InlineEntity = new OwnedInlineEntity
                                                                {
                                                                   IntColumn = 0,
                                                                   StringColumn = null
                                                                }
                                              });
      }
   }
}
