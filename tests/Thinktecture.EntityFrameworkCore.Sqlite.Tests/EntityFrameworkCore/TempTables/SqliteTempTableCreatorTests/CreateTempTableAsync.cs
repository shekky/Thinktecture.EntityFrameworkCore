using System;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using FluentAssertions;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;
using Moq;
using Thinktecture.EntityFrameworkCore.BulkOperations;
using Thinktecture.TestDatabaseContext;
using Xunit;
using Xunit.Abstractions;

namespace Thinktecture.EntityFrameworkCore.TempTables.SqliteTempTableCreatorTests
{
   // ReSharper disable once InconsistentNaming
   public class CreateTempTableAsync : IntegrationTestsBase
   {
      private readonly Mock<ISqlGenerationHelper> _sqlGenerationHelperMock;
      private readonly Mock<IRelationalTypeMappingSource> _relationalTypeMappingSourceMock;
      private readonly TempTableCreationOptions _optionsWithNonUniqueName;

      private SqliteTempTableCreator _sut;

      private SqliteTempTableCreator SUT => _sut ??= new SqliteTempTableCreator(ActDbContext.GetService<ICurrentDbContext>(),
                                                                                ActDbContext.GetService<IDiagnosticsLogger<DbLoggerCategory.Query>>(),
                                                                                _sqlGenerationHelperMock.Object,
                                                                                _relationalTypeMappingSourceMock.Object);

      public CreateTempTableAsync(ITestOutputHelper testOutputHelper)
         : base(testOutputHelper)
      {
         _sqlGenerationHelperMock = new Mock<ISqlGenerationHelper>();
         _sqlGenerationHelperMock.Setup(h => h.DelimitIdentifier(It.IsAny<string>(), It.IsAny<string>()))
                                 .Returns<string, string>((name, schema) => schema == null ? $"\"{name}\"" : $"\"{schema}\".\"{name}\"");
         _sqlGenerationHelperMock.Setup(h => h.DelimitIdentifier(It.IsAny<string>()))
                                 .Returns<string>(name => $"\"{name}\"");
         _relationalTypeMappingSourceMock = new Mock<IRelationalTypeMappingSource>();

         _optionsWithNonUniqueName = new TempTableCreationOptions { TableNameProvider = DefaultTempTableNameProvider.Instance, CreatePrimaryKey = false };
      }

      [Fact]
      public async Task Should_create_temp_table_for_keyless_entity()
      {
         ConfigureModel = builder => builder.ConfigureTempTableEntity<CustomTempTable>();

         // ReSharper disable once RedundantArgumentDefaultValue
         await using var tempTable = await SUT.CreateTempTableAsync(ActDbContext.GetEntityType<CustomTempTable>(), _optionsWithNonUniqueName);

         var columns = AssertDbContext.GetTempTableColumns<CustomTempTable>().ToList();
         columns.Should().HaveCount(2);

         ValidateColumn(columns[0], nameof(CustomTempTable.Column1), "INTEGER", false);
         ValidateColumn(columns[1], nameof(CustomTempTable.Column2), "TEXT", true);
      }

      [Fact]
      public async Task Should_delete_temp_table_on_dispose_if_DropTableOnDispose_is_true()
      {
         ConfigureModel = builder => builder.ConfigureTempTableEntity<CustomTempTable>();

         _optionsWithNonUniqueName.DropTableOnDispose = true;

         // ReSharper disable once UseAwaitUsing
         await using (await SUT.CreateTempTableAsync(ActDbContext.GetEntityType<CustomTempTable>(), _optionsWithNonUniqueName).ConfigureAwait(false))
         {
         }

         AssertDbContext.GetTempTableColumns<CustomTempTable>().ToList()
                        .Should().HaveCount(0);
      }

      [Fact]
      public async Task Should_delete_temp_table_on_disposeAsync_if_DropTableOnDispose_is_true()
      {
         ConfigureModel = builder => builder.ConfigureTempTableEntity<CustomTempTable>();

         _optionsWithNonUniqueName.DropTableOnDispose = true;

         await using (await SUT.CreateTempTableAsync(ActDbContext.GetEntityType<CustomTempTable>(), _optionsWithNonUniqueName).ConfigureAwait(false))
         {
         }

         AssertDbContext.GetTempTableColumns<CustomTempTable>().ToList()
                        .Should().HaveCount(0);
      }

      [Fact]
      public async Task Should_not_delete_temp_table_on_dispose_if_DropTableOnDispose_is_false()
      {
         ConfigureModel = builder => builder.ConfigureTempTableEntity<CustomTempTable>();

         _optionsWithNonUniqueName.DropTableOnDispose = false;

         // ReSharper disable once UseAwaitUsing
         await using (await SUT.CreateTempTableAsync(ActDbContext.GetEntityType<CustomTempTable>(), _optionsWithNonUniqueName).ConfigureAwait(false))
         {
         }

         var columns = AssertDbContext.GetTempTableColumns<CustomTempTable>().ToList();
         columns.Should().HaveCount(2);

         ValidateColumn(columns[0], nameof(CustomTempTable.Column1), "INTEGER", false);
         ValidateColumn(columns[1], nameof(CustomTempTable.Column2), "TEXT", true);
      }

      [Fact]
      public async Task Should_not_delete_temp_table_on_disposeAsync_if_DropTableOnDispose_is_false()
      {
         ConfigureModel = builder => builder.ConfigureTempTableEntity<CustomTempTable>();

         _optionsWithNonUniqueName.DropTableOnDispose = false;

         await using (await SUT.CreateTempTableAsync(ActDbContext.GetEntityType<CustomTempTable>(), _optionsWithNonUniqueName).ConfigureAwait(false))
         {
         }

         var columns = AssertDbContext.GetTempTableColumns<CustomTempTable>().ToList();
         columns.Should().HaveCount(2);

         ValidateColumn(columns[0], nameof(CustomTempTable.Column1), "INTEGER", false);
         ValidateColumn(columns[1], nameof(CustomTempTable.Column2), "TEXT", true);
      }

      [Fact]
      public async Task Should_create_temp_table_with_reusable_name()
      {
         ConfigureModel = builder => builder.ConfigureTempTableEntity<CustomTempTable>();

         var options = new TempTableCreationOptions { TableNameProvider = ReusingTempTableNameProvider.Instance };

         // ReSharper disable once RedundantArgumentDefaultValue
         await using var tempTable = await SUT.CreateTempTableAsync(ActDbContext.GetEntityType<CustomTempTable>(), options).ConfigureAwait(false);

         var columns = AssertDbContext.GetTempTableColumns("#CustomTempTable_1").ToList();
         columns.Should().HaveCount(2);

         ValidateColumn(columns[0], nameof(CustomTempTable.Column1), "INTEGER", false);
         ValidateColumn(columns[1], nameof(CustomTempTable.Column2), "TEXT", true);
      }

      [Fact]
      public async Task Should_reuse_name_after_it_is_freed()
      {
         ConfigureModel = builder => builder.ConfigureTempTableEntity<CustomTempTable>();

         var options = new TempTableCreationOptions { TableNameProvider = ReusingTempTableNameProvider.Instance };

         // ReSharper disable once RedundantArgumentDefaultValue
         await using (await SUT.CreateTempTableAsync(ActDbContext.GetEntityType<CustomTempTable>(), options).ConfigureAwait(false))
         {
         }

         await using var tempTable = await SUT.CreateTempTableAsync(ActDbContext.GetEntityType<CustomTempTable>(), options).ConfigureAwait(false);

         var columns = AssertDbContext.GetTempTableColumns("#CustomTempTable_1").ToList();
         columns.Should().HaveCount(2);

         ValidateColumn(columns[0], nameof(CustomTempTable.Column1), "INTEGER", false);
         ValidateColumn(columns[1], nameof(CustomTempTable.Column2), "TEXT", true);
      }

      [Fact]
      public async Task Should_reuse_name_after_it_is_freed_although_previously_not_dropped()
      {
         ConfigureModel = builder => builder.ConfigureTempTableEntity<CustomTempTable>();

         var options = new TempTableCreationOptions
                       {
                          TableNameProvider = ReusingTempTableNameProvider.Instance,
                          DropTableOnDispose = false
                       };

         // ReSharper disable once RedundantArgumentDefaultValue
         await using (await SUT.CreateTempTableAsync(ActDbContext.GetEntityType<CustomTempTable>(), options).ConfigureAwait(false))
         {
         }

         options.TruncateTableIfExists = true;
         await using var tempTable = await SUT.CreateTempTableAsync(ActDbContext.GetEntityType<CustomTempTable>(), options).ConfigureAwait(false);

         var columns = AssertDbContext.GetTempTableColumns("#CustomTempTable_1").ToList();
         columns.Should().HaveCount(2);

         ValidateColumn(columns[0], nameof(CustomTempTable.Column1), "INTEGER", false);
         ValidateColumn(columns[1], nameof(CustomTempTable.Column2), "TEXT", true);

         AssertDbContext.GetTempTableColumns("#CustomTempTable_2").ToList()
                        .Should().HaveCount(0);
      }

      [Fact]
      public async Task Should_not_reuse_name_before_it_is_freed()
      {
         ConfigureModel = builder => builder.ConfigureTempTableEntity<CustomTempTable>();

         var options = new TempTableCreationOptions { TableNameProvider = ReusingTempTableNameProvider.Instance };

         // ReSharper disable once RedundantArgumentDefaultValue
         await using (await SUT.CreateTempTableAsync(ActDbContext.GetEntityType<CustomTempTable>(), options).ConfigureAwait(false))
         {
            await SUT.CreateTempTableAsync(ActDbContext.GetEntityType<CustomTempTable>(), options).ConfigureAwait(false);
         }

         var columns = AssertDbContext.GetTempTableColumns("#CustomTempTable_2").ToList();
         columns.Should().HaveCount(2);

         ValidateColumn(columns[0], nameof(CustomTempTable.Column1), "INTEGER", false);
         ValidateColumn(columns[1], nameof(CustomTempTable.Column2), "TEXT", true);
      }

      [Fact]
      public async Task Should_reuse_name_in_sorted_order()
      {
         ConfigureModel = builder => builder.ConfigureTempTableEntity<CustomTempTable>();

         var options = new TempTableCreationOptions { TableNameProvider = ReusingTempTableNameProvider.Instance };

         // #CustomTempTable_1
         await using (await SUT.CreateTempTableAsync(ActDbContext.GetEntityType<CustomTempTable>(), options).ConfigureAwait(false))
         {
            // #CustomTempTable_2
            await using (await SUT.CreateTempTableAsync(ActDbContext.GetEntityType<CustomTempTable>(), options).ConfigureAwait(false))
            {
            }
         }

         // #CustomTempTable_1
         await using var tempTable = await SUT.CreateTempTableAsync(ActDbContext.GetEntityType<CustomTempTable>(), options).ConfigureAwait(false);

         var columns = AssertDbContext.GetTempTableColumns("#CustomTempTable_1").ToList();
         columns.Should().HaveCount(2);

         ValidateColumn(columns[0], nameof(CustomTempTable.Column1), "INTEGER", false);
         ValidateColumn(columns[1], nameof(CustomTempTable.Column2), "TEXT", true);
      }

      [Fact]
      public async Task Should_create_temp_table_with_provided_column_only()
      {
         ConfigureModel = builder => builder.ConfigureTempTableEntity<CustomTempTable>();

         _optionsWithNonUniqueName.MembersToInclude = EntityMembersProvider.From<CustomTempTable>(t => t.Column1);

         // ReSharper disable once RedundantArgumentDefaultValue
         await using var tempTable = await SUT.CreateTempTableAsync(ActDbContext.GetEntityType<CustomTempTable>(), _optionsWithNonUniqueName);

         var columns = AssertDbContext.GetTempTableColumns<CustomTempTable>().ToList();
         columns.Should().HaveCount(1);

         ValidateColumn(columns[0], nameof(CustomTempTable.Column1), "INTEGER", false);
      }

      [Fact]
      public async Task Should_create_pk_if_options_flag_is_set()
      {
         _optionsWithNonUniqueName.CreatePrimaryKey = true;

         ConfigureModel = builder => builder.ConfigureTempTable<int, string>().Property(s => s.Column2).HasMaxLength(100);

         // ReSharper disable once RedundantArgumentDefaultValue
         await using var tempTable = await SUT.CreateTempTableAsync(ActDbContext.GetEntityType<TempTable<int, string>>(), _optionsWithNonUniqueName);

         var keyColumns = await AssertDbContext.GetTempTableKeyColumns<TempTable<int, string>>().ToListAsync();
         keyColumns.Should().HaveCount(2);
         keyColumns[0].Name.Should().Be(nameof(TempTable<int, string>.Column1));
         keyColumns[1].Name.Should().Be(nameof(TempTable<int, string>.Column2));
      }

      [Fact]
      public void Should_throw_if_some_pk_columns_are_missing()
      {
         _optionsWithNonUniqueName.CreatePrimaryKey = true;
         _optionsWithNonUniqueName.MembersToInclude = EntityMembersProvider.From<CustomTempTable>(t => t.Column1);

         ConfigureModel = builder => builder.ConfigureTempTableEntity<CustomTempTable>().Property(s => s.Column2).HasMaxLength(100);

         // ReSharper disable once RedundantArgumentDefaultValue
         SUT.Awaiting(sut => SUT.CreateTempTableAsync(ActDbContext.GetEntityType<CustomTempTable>(), _optionsWithNonUniqueName))
            .Should().Throw<ArgumentException>().WithMessage("Cannot create PRIMARY KEY because not all key columns are part of the temp table. Missing columns: Column2.");
      }

      [Fact]
      public async Task Should_open_connection()
      {
         await using var con = new SqliteConnection("DataSource=:memory:");

         var builder = CreateOptionsBuilder(con);

         await using var ctx = new TestDbContext(builder.Options);

         ctx.Database.GetDbConnection().State.Should().Be(ConnectionState.Closed);

         // ReSharper disable once RedundantArgumentDefaultValue
         await using var tempTable = await ctx.GetService<ITempTableCreator>()
                                              .CreateTempTableAsync(ctx.GetEntityType<TestEntity>(), _optionsWithNonUniqueName);

         ctx.Database.GetDbConnection().State.Should().Be(ConnectionState.Open);
      }

      [Fact]
      public async Task Should_return_reference_to_be_able_to_close_connection()
      {
         await using var con = new SqliteConnection("DataSource=:memory:");

         var builder = CreateOptionsBuilder(con);

         await using var ctx = new TestDbContext(builder.Options);

         ctx.Database.GetDbConnection().State.Should().Be(ConnectionState.Closed);

         // ReSharper disable once RedundantArgumentDefaultValue
         var tempTableReference = await ctx.GetService<ITempTableCreator>()
                                           .CreateTempTableAsync(ctx.GetEntityType<TestEntity>(), _optionsWithNonUniqueName);
         await tempTableReference.DisposeAsync();

         ctx.Database.GetDbConnection().State.Should().Be(ConnectionState.Closed);
      }

      [Fact]
      public async Task Should_return_reference_to_be_able_to_close_connection_event_if_ctx_is_disposed()
      {
         await using var con = new SqliteConnection("DataSource=:memory:");

         var builder = CreateOptionsBuilder(con);

         ITempTableReference tempTableReference;

         await using (var ctx = new TestDbContext(builder.Options))
         {
            ctx.Database.GetDbConnection().State.Should().Be(ConnectionState.Closed);

            // ReSharper disable once RedundantArgumentDefaultValue
            tempTableReference = await ctx.GetService<ITempTableCreator>()
                                          .CreateTempTableAsync(ctx.GetEntityType<TestEntity>(), _optionsWithNonUniqueName);
         }

         con.State.Should().Be(ConnectionState.Open);
         await tempTableReference.DisposeAsync();
         con.State.Should().Be(ConnectionState.Closed);
      }

      [Fact]
      public async Task Should_return_table_ref_that_does_nothing_after_connection_is_disposed()
      {
         await using var con = new SqliteConnection("DataSource=:memory:");

         var builder = CreateOptionsBuilder(con);

         await using var ctx = new TestDbContext(builder.Options);

         ctx.Database.GetDbConnection().State.Should().Be(ConnectionState.Closed);

         // ReSharper disable once RedundantArgumentDefaultValue
         var tempTableReference = await ctx.GetService<ITempTableCreator>()
                                           .CreateTempTableAsync(ctx.GetEntityType<TestEntity>(), _optionsWithNonUniqueName);
         con.Dispose();

         con.State.Should().Be(ConnectionState.Closed);
         tempTableReference.Dispose();
         con.State.Should().Be(ConnectionState.Closed);
      }

      [Fact]
      public async Task Should_return_table_ref_that_does_nothing_after_connection_is_disposedAsync()
      {
         await using var con = new SqliteConnection("DataSource=:memory:");

         var builder = CreateOptionsBuilder(con);

         await using var ctx = new TestDbContext(builder.Options);

         ctx.Database.GetDbConnection().State.Should().Be(ConnectionState.Closed);

         // ReSharper disable once RedundantArgumentDefaultValue
         var tempTableReference = await ctx.GetService<ITempTableCreator>()
                                           .CreateTempTableAsync(ctx.GetEntityType<TestEntity>(), _optionsWithNonUniqueName);
         await con.DisposeAsync();

         con.State.Should().Be(ConnectionState.Closed);
         await tempTableReference.DisposeAsync();
         con.State.Should().Be(ConnectionState.Closed);
      }

      [Fact]
      public async Task Should_return_table_ref_that_does_nothing_after_connection_is_closed()
      {
         await using var con = new SqliteConnection("DataSource=:memory:");

         var builder = CreateOptionsBuilder(con);

         await using var ctx = new TestDbContext(builder.Options);

         ctx.Database.GetDbConnection().State.Should().Be(ConnectionState.Closed);

         // ReSharper disable once RedundantArgumentDefaultValue
         await using var tempTableReference = await ctx.GetService<ITempTableCreator>()
                                                       .CreateTempTableAsync(ctx.GetEntityType<TestEntity>(), _optionsWithNonUniqueName);
         con.Close();

         tempTableReference.Dispose();
         con.State.Should().Be(ConnectionState.Closed);
      }

      [Fact]
      public async Task Should_return_reference_to_remove_temp_table()
      {
         ConfigureModel = builder => builder.ConfigureTempTableEntity<CustomTempTable>();

         // ReSharper disable once RedundantArgumentDefaultValue
         await using var tempTableReference = await SUT.CreateTempTableAsync(ActDbContext.GetEntityType<CustomTempTable>(), _optionsWithNonUniqueName);
         await tempTableReference.DisposeAsync();

         var columns = AssertDbContext.GetTempTableColumns<CustomTempTable>().ToList();
         columns.Should().BeEmpty();
      }

      [Fact]
      public async Task Should_create_temp_table_for_entityType()
      {
         // ReSharper disable once RedundantArgumentDefaultValue
         await using var tempTable = await SUT.CreateTempTableAsync(ActDbContext.GetEntityType<TestEntity>(), _optionsWithNonUniqueName);

         var columns = AssertDbContext.GetTempTableColumns<TestEntity>().OrderBy(c => c.Name).ToList();
         columns.Should().HaveCount(6);

         ValidateColumn(columns[0], nameof(TestEntity.ConvertibleClass), "INTEGER", true);
         ValidateColumn(columns[1], nameof(TestEntity.Count), "INTEGER", false);
         ValidateColumn(columns[2], nameof(TestEntity.Id), "TEXT", false);
         ValidateColumn(columns[3], nameof(TestEntity.Name), "TEXT", true);
         ValidateColumn(columns[4], nameof(TestEntity.PropertyWithBackingField), "INTEGER", false);
         ValidateColumn(columns[5], "_privateField", "INTEGER", false);
      }

      [Fact]
      public void Should_throw_if_temp_table_is_not_introduced()
      {
         SUT.Awaiting(c => c.CreateTempTableAsync(ActDbContext.GetEntityType<TempTable<int>>(), _optionsWithNonUniqueName))
            .Should().Throw<ArgumentException>();
      }

      [Fact]
      public async Task Should_create_temp_table_with_one_column()
      {
         ConfigureModel = builder => builder.ConfigureTempTable<int>();

         await using var tempTable = await SUT.CreateTempTableAsync(ActDbContext.GetEntityType<TempTable<int>>(), _optionsWithNonUniqueName);

         AssertDbContext.GetTempTableColumns<TempTable<int>>().ToList().Should().HaveCount(1);
      }

      [Fact]
      public async Task Should_create_temp_table_without_primary_key()
      {
         ConfigureModel = builder => builder.ConfigureTempTable<int>();
         _optionsWithNonUniqueName.CreatePrimaryKey = false;

         await using var tempTable = await SUT.CreateTempTableAsync(ActDbContext.GetEntityType<TempTable<int>>(), _optionsWithNonUniqueName);

         var constraints = await AssertDbContext.GetTempTableKeyColumns<TempTable<int>>().ToListAsync();
         constraints.Should().BeEmpty();
      }

      [Fact]
      public async Task Should_create_temp_table_with_int()
      {
         ConfigureModel = builder => builder.ConfigureTempTable<int>();

         await using var tempTable = await SUT.CreateTempTableAsync(ActDbContext.GetEntityType<TempTable<int>>(), _optionsWithNonUniqueName);

         var columns = AssertDbContext.GetTempTableColumns<TempTable<int>>().ToList();
         ValidateColumn(columns[0], nameof(TempTable<int>.Column1), "INTEGER", false);
      }

      [Fact]
      public async Task Should_create_temp_table_with_nullable_int()
      {
         ConfigureModel = builder => builder.ConfigureTempTable<int?>();

         await using var tempTable = await SUT.CreateTempTableAsync(ActDbContext.GetEntityType<TempTable<int?>>(), _optionsWithNonUniqueName);

         var columns = AssertDbContext.GetTempTableColumns<TempTable<int?>>().ToList();
         ValidateColumn(columns[0], nameof(TempTable<int?>.Column1), "INTEGER", true);
      }

      [Fact]
      public async Task Should_create_make_nullable_int_to_non_nullable_if_set_via_modelbuilder()
      {
         ConfigureModel = builder => builder.ConfigureTempTable<int?>().Property(t => t.Column1).IsRequired();

         await using var tempTable = await SUT.CreateTempTableAsync(ActDbContext.GetEntityType<TempTable<int?>>(), _optionsWithNonUniqueName);

         var columns = AssertDbContext.GetTempTableColumns<TempTable<int?>>().ToList();
         ValidateColumn(columns[0], nameof(TempTable<int?>.Column1), "INTEGER", false);
      }

      [Fact]
      public async Task Should_create_temp_table_with_double()
      {
         ConfigureModel = builder => builder.ConfigureTempTable<double>();

         await using var tempTable = await SUT.CreateTempTableAsync(ActDbContext.GetEntityType<TempTable<double>>(), _optionsWithNonUniqueName);

         var columns = AssertDbContext.GetTempTableColumns<TempTable<double>>().ToList();
         ValidateColumn(columns[0], nameof(TempTable<double>.Column1), "REAL", false);
      }

      [Fact]
      public async Task Should_create_temp_table_with_decimal()
      {
         ConfigureModel = builder => builder.ConfigureTempTable<decimal>();

         await using var tempTable = await SUT.CreateTempTableAsync(ActDbContext.GetEntityType<TempTable<decimal>>(), _optionsWithNonUniqueName);

         var columns = AssertDbContext.GetTempTableColumns<TempTable<decimal>>().ToList();
         ValidateColumn(columns[0], nameof(TempTable<decimal>.Column1), "TEXT", false); // decimal is stored as TEXT (see SqliteTypeMappingSource)
      }

      [Fact]
      public async Task Should_create_temp_table_with_decimal_with_explicit_precision()
      {
         ConfigureModel = builder => builder.ConfigureTempTable<decimal>().Property(t => t.Column1).HasColumnType("decimal(20,5)");

         await using var tempTable = await SUT.CreateTempTableAsync(ActDbContext.GetEntityType<TempTable<decimal>>(), _optionsWithNonUniqueName);

         var columns = AssertDbContext.GetTempTableColumns<TempTable<decimal>>().ToList();
         ValidateColumn(columns[0], nameof(TempTable<decimal>.Column1), "decimal(20,5)", false);
      }

      [Fact]
      public async Task Should_create_temp_table_with_bool()
      {
         ConfigureModel = builder => builder.ConfigureTempTable<bool>();

         await using var tempTable = await SUT.CreateTempTableAsync(ActDbContext.GetEntityType<TempTable<bool>>(), _optionsWithNonUniqueName);

         var columns = AssertDbContext.GetTempTableColumns<TempTable<bool>>().ToList();
         ValidateColumn(columns[0], nameof(TempTable<bool>.Column1), "INTEGER", false);
      }

      [Fact]
      public async Task Should_create_temp_table_with_string()
      {
         ConfigureModel = builder => builder.ConfigureTempTable<string>();

         await using var tempTable = await SUT.CreateTempTableAsync(ActDbContext.GetEntityType<TempTable<string>>(), _optionsWithNonUniqueName);

         var columns = AssertDbContext.GetTempTableColumns<TempTable<string>>().ToList();
         ValidateColumn(columns[0], nameof(TempTable<string>.Column1), "TEXT", false);
      }

      [Fact]
      public async Task Should_create_temp_table_with_string_with_max_length()
      {
         ConfigureModel = builder => builder.ConfigureTempTable<string>().Property(t => t.Column1).HasMaxLength(50);

         await using var tempTable = await SUT.CreateTempTableAsync(ActDbContext.GetEntityType<TempTable<string>>(), _optionsWithNonUniqueName);

         var columns = AssertDbContext.GetTempTableColumns<TempTable<string>>().ToList();
         ValidateColumn(columns[0], nameof(TempTable<string>.Column1), "TEXT", false);
      }

      [Fact]
      public async Task Should_create_temp_table_with_2_columns()
      {
         ConfigureModel = builder => builder.ConfigureTempTable<int, string>();

         await using var tempTable = await SUT.CreateTempTableAsync(ActDbContext.GetEntityType<TempTable<int, string>>(), _optionsWithNonUniqueName);

         var columns = AssertDbContext.GetTempTableColumns<TempTable<int, string>>().ToList();
         columns.Should().HaveCount(2);

         ValidateColumn(columns[0], nameof(TempTable<int, string>.Column1), "INTEGER", false);
         ValidateColumn(columns[1], nameof(TempTable<int, string>.Column2), "TEXT", false);
      }

      [Fact]
      public async Task Should_create_temp_table_for_entity_with_inlined_owned_type()
      {
         await using var tempTable = await SUT.CreateTempTableAsync(ActDbContext.GetEntityType<TestEntityOwningInlineEntity>(), _optionsWithNonUniqueName);

         var columns = AssertDbContext.GetTempTableColumns<TestEntityOwningInlineEntity>().ToList();
         columns.Should().HaveCount(3);

         ValidateColumn(columns[0], nameof(TestEntityOwningInlineEntity.Id), "TEXT", false);
         ValidateColumn(columns[1], $"{nameof(TestEntityOwningInlineEntity.InlineEntity)}_{nameof(TestEntityOwningInlineEntity.InlineEntity.IntColumn)}", "INTEGER", true);
         ValidateColumn(columns[2], $"{nameof(TestEntityOwningInlineEntity.InlineEntity)}_{nameof(TestEntityOwningInlineEntity.InlineEntity.StringColumn)}", "TEXT", true);
      }

      [Fact]
      public async Task Should_create_temp_table_for_entity_by_selecting_inlined_owned_type_as_whole()
      {
         _optionsWithNonUniqueName.MembersToInclude = EntityMembersProvider.From<TestEntityOwningInlineEntity>(e => new
                                                                                                                    {
                                                                                                                       e.Id,
                                                                                                                       e.InlineEntity
                                                                                                                    });
         await using var tempTable = await SUT.CreateTempTableAsync(ActDbContext.GetEntityType<TestEntityOwningInlineEntity>(), _optionsWithNonUniqueName);

         var columns = AssertDbContext.GetTempTableColumns<TestEntityOwningInlineEntity>().ToList();
         columns.Should().HaveCount(3);

         ValidateColumn(columns[0], nameof(TestEntityOwningInlineEntity.Id), "TEXT", false);
         ValidateColumn(columns[1], $"{nameof(TestEntityOwningInlineEntity.InlineEntity)}_{nameof(TestEntityOwningInlineEntity.InlineEntity.IntColumn)}", "INTEGER", true);
         ValidateColumn(columns[2], $"{nameof(TestEntityOwningInlineEntity.InlineEntity)}_{nameof(TestEntityOwningInlineEntity.InlineEntity.StringColumn)}", "TEXT", true);
      }

      [Fact]
      public async Task Should_create_temp_table_for_entity_with_separated_owned_type()
      {
         var ownerEntityType = ActDbContext.GetEntityType<TestEntityOwningOneSeparateEntity>();
         await using var tempTable = await SUT.CreateTempTableAsync(ownerEntityType, _optionsWithNonUniqueName);

         var columns = AssertDbContext.GetTempTableColumns<TestEntityOwningOneSeparateEntity>().ToList();
         columns.Should().HaveCount(1);

         ValidateColumn(columns[0], nameof(TestEntityOwningOneSeparateEntity.Id), "TEXT", false);

         var ownedTypeEntityType = ownerEntityType.GetNavigations().Single().GetTargetType();
         await using var ownedTempTable = await SUT.CreateTempTableAsync(ownedTypeEntityType, _optionsWithNonUniqueName);

         columns = AssertDbContext.GetTempTableColumns(ownedTypeEntityType).ToList();
         columns.Should().HaveCount(3);
         ValidateColumn(columns[0], $"{nameof(TestEntityOwningOneSeparateEntity)}{nameof(TestEntityOwningOneSeparateEntity.Id)}", "TEXT", false);
         ValidateColumn(columns[1], nameof(OwnedSeparateEntity.IntColumn), "INTEGER", false);
         ValidateColumn(columns[2], nameof(OwnedSeparateEntity.StringColumn), "TEXT", true);
      }

      [Fact]
      public void Should_throw_when_selecting_separated_owned_type_as_whole()
      {
         _optionsWithNonUniqueName.MembersToInclude = EntityMembersProvider.From<TestEntityOwningOneSeparateEntity>(e => new
                                                                                                                         {
                                                                                                                            e.Id,
                                                                                                                            e.SeparateEntity
                                                                                                                         });
         SUT.Awaiting(sut => sut.CreateTempTableAsync(ActDbContext.GetEntityType<TestEntityOwningOneSeparateEntity>(), _optionsWithNonUniqueName))
            .Should().Throw<NotSupportedException>();
      }

      [Fact]
      public async Task Should_create_temp_table_for_entity_with_many_owned_types()
      {
         var ownerEntityType = ActDbContext.GetEntityType<TestEntityOwningManyEntities>();
         await using var tempTable = await SUT.CreateTempTableAsync(ownerEntityType, _optionsWithNonUniqueName);

         var columns = AssertDbContext.GetTempTableColumns<TestEntityOwningManyEntities>().ToList();
         columns.Should().HaveCount(1);

         ValidateColumn(columns[0], nameof(TestEntityOwningManyEntities.Id), "TEXT", false);

         var ownedTypeEntityType = ownerEntityType.GetNavigations().Single().GetTargetType();
         await using var ownedTempTable = await SUT.CreateTempTableAsync(ownedTypeEntityType, _optionsWithNonUniqueName);

         columns = AssertDbContext.GetTempTableColumns(ownedTypeEntityType).ToList();
         columns.Should().HaveCount(4);
         ValidateColumn(columns[0], $"{nameof(TestEntityOwningManyEntities)}{nameof(TestEntityOwningManyEntities.Id)}", "TEXT", false);
         ValidateColumn(columns[1], "Id", "INTEGER", false);
         ValidateColumn(columns[2], nameof(OwnedSeparateEntity.IntColumn), "INTEGER", false);
         ValidateColumn(columns[3], nameof(OwnedSeparateEntity.StringColumn), "TEXT", true);
      }

      private static void ValidateColumn(SqliteTableInfo column, string name, string type, bool isNullable)
      {
         if (column == null)
            throw new ArgumentNullException(nameof(column));

         column.Name.Should().Be(name);
         column.Type.Should().Be(type);
         column.NotNull.Should().Be(isNullable ? 0 : 1);
      }
   }
}
