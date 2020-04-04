/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.prestosql.plugin.oracle;

import io.prestosql.Session;
import io.prestosql.testing.AbstractTestDistributedQueries;
import io.prestosql.testing.MaterializedResult;
import io.prestosql.testing.QueryRunner;
import io.prestosql.testing.sql.TestTable;
import io.prestosql.tpch.TpchTable;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.testing.MaterializedResult.resultBuilder;
import static java.lang.String.format;
import static java.util.stream.Collectors.joining;
import static java.util.stream.IntStream.range;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestOracleDistributedQueries
        extends AbstractTestDistributedQueries
{
    private OracleServer oracleServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        this.oracleServer = new OracleServer();
        return createOracleQueryRunner(oracleServer, TpchTable.getTables());
    }

    protected QueryRunner createOracleQueryRunner(OracleServer server, List<TpchTable<?>> tables)
            throws Exception
    {
        return OracleQueryRunner.createOracleQueryRunner(server, tables);
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
    {
        if (oracleServer != null) {
            oracleServer.close();
        }
    }

    @Override
    protected boolean supportsViews()
    {
        return false;
    }

    @Override
    protected boolean supportsArrays()
    {
        return false;
    }

    @Override
    public void testCommentTable()
    {
        // table comment not supported
    }

    @Override
    public void testDelete()
    {
        // delete is not supported
    }

    @Override
    public void testCreateSchema()
    {
        // schema creation is not supported
    }

    @Override
    protected String dataMappingTableName(String prestoTypeName)
    {
        return "presto_tmp_" + System.nanoTime();
    }

    @Override
    protected Optional<DataMappingTestSetup> filterDataMappingSmokeTestData(DataMappingTestSetup dataMappingTestSetup)
    {
        String typeName = dataMappingTestSetup.getPrestoTypeName();
        if (typeName.equals("timestamp with time zone")
                || typeName.equals("time")
                || typeName.equals("varbinary")
                || typeName.equals("char(3)")) {
            return Optional.empty();
        }

        return Optional.of(dataMappingTestSetup);
    }

    @Override
    protected TestTable createTableWithDefaultColumns()
    {
        return new TestTable(
                oracleServer::execute,
                "tpch.table",
                "(col_required decimal(20,0) NOT NULL," +
                        "col_nullable decimal(20,0)," +
                        "col_default decimal(20,0) DEFAULT 43," +
                        "col_nonnull_default decimal(20,0) DEFAULT 42 NOT NULL ," +
                        "col_required2 decimal(20,0) NOT NULL)");
    }

    @Test
    @Override
    public void testLargeIn()
    {
        int numberOfElements = 1000;
        String longValues = range(0, numberOfElements)
                .mapToObj(Integer::toString)
                .collect(joining(", "));
        assertQuery("SELECT orderkey FROM orders WHERE orderkey IN (" + longValues + ")");
        assertQuery("SELECT orderkey FROM orders WHERE orderkey NOT IN (" + longValues + ")");

        assertQuery("SELECT orderkey FROM orders WHERE orderkey IN (mod(1000, orderkey), " + longValues + ")");
        assertQuery("SELECT orderkey FROM orders WHERE orderkey NOT IN (mod(1000, orderkey), " + longValues + ")");

        String arrayValues = range(0, numberOfElements)
                .mapToObj(i -> format("ARRAY[%s, %s, %s]", i, i + 1, i + 2))
                .collect(joining(", "));
        assertQuery("SELECT ARRAY[0, 0, 0] in (ARRAY[0, 0, 0], " + arrayValues + ")", "values true");
        assertQuery("SELECT ARRAY[0, 0, 0] in (" + arrayValues + ")", "values false");
    }

    @Test
    @Override
    public void testCreateTableAsSelect()
    {
        assertUpdate("CREATE TABLE IF NOT EXISTS test_ctas AS SELECT name, regionkey FROM nation", "SELECT count(*) FROM nation");
        assertTableColumnNames("test_ctas", "name", "regionkey");
        assertUpdate("DROP TABLE test_ctas");

        assertUpdate("CREATE TABLE IF NOT EXISTS nation AS SELECT orderkey, discount FROM lineitem", 0);
        assertTableColumnNames("nation", "nationkey", "name", "regionkey", "comment");

        assertCreateTableAsSelect(
                "test_select",
                "SELECT orderdate, orderkey, totalprice FROM orders",
                "SELECT count(*) FROM orders");

        assertCreateTableAsSelect(
                "test_group",
                "SELECT orderstatus, sum(totalprice) x FROM orders GROUP BY orderstatus",
                "SELECT count(DISTINCT orderstatus) FROM orders");

        assertCreateTableAsSelect(
                "test_join",
                "SELECT count(*) x FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey",
                "SELECT 1");

        assertCreateTableAsSelect(
                "test_limit",
                "SELECT orderkey FROM orders ORDER BY orderkey LIMIT 10",
                "SELECT 10");

        // this is comment because presto creates a table of varchar(1) and in oracle this unicode occupy 3 char
        // we should try to get bytes instead of size ??
        /*
        assertCreateTableAsSelect(
                "test_unicode",
                "SELECT '\u2603' unicode",
                "SELECT 1");
        */
        assertCreateTableAsSelect(
                "test_with_data",
                "SELECT * FROM orders WITH DATA",
                "SELECT * FROM orders",
                "SELECT count(*) FROM orders");

        assertCreateTableAsSelect(
                "test_with_no_data",
                "SELECT * FROM orders WITH NO DATA",
                "SELECT * FROM orders LIMIT 0",
                "SELECT 0");

        assertCreateTableAsSelect(
                "test_union_all",
                "SELECT orderdate, orderkey, totalprice FROM orders WHERE orderkey % 2 = 0 UNION ALL " +
                        "SELECT orderdate, orderkey, totalprice FROM orders WHERE orderkey % 2 = 1",
                "SELECT orderdate, orderkey, totalprice FROM orders",
                "SELECT count(*) FROM orders");

        assertCreateTableAsSelect(
                Session.builder(getSession()).setSystemProperty("redistribute_writes", "true").build(),
                "test_union_all",
                "SELECT CAST(orderdate AS DATE) orderdate, orderkey, totalprice FROM orders UNION ALL " +
                        "SELECT DATE '2000-01-01', 1234567890, 1.23",
                "SELECT orderdate, orderkey, totalprice FROM orders UNION ALL " +
                        "SELECT DATE '2000-01-01', 1234567890, 1.23",
                "SELECT count(*) + 1 FROM orders");

        assertCreateTableAsSelect(
                Session.builder(getSession()).setSystemProperty("redistribute_writes", "false").build(),
                "test_union_all",
                "SELECT CAST(orderdate AS DATE) orderdate, orderkey, totalprice FROM orders UNION ALL " +
                        "SELECT DATE '2000-01-01', 1234567890, 1.23",
                "SELECT orderdate, orderkey, totalprice FROM orders UNION ALL " +
                        "SELECT DATE '2000-01-01', 1234567890, 1.23",
                "SELECT count(*) + 1 FROM orders");

        // this as private access
        /*
        assertExplainAnalyze("EXPLAIN ANALYZE CREATE TABLE analyze_test AS SELECT orderstatus FROM orders");
        assertQuery("SELECT * from analyze_test", "SELECT orderstatus FROM orders");
        assertUpdate("DROP TABLE analyze_test");
        */
    }

    @Test
    @Override
    public void testCreateTable()
    {
        assertUpdate("CREATE TABLE test_create (a bigint, b double, c varchar)");
        assertTrue(getQueryRunner().tableExists(getSession(), "test_create"));
        assertTableColumnNames("test_create", "a", "b", "c");

        assertUpdate("DROP TABLE test_create");
        assertFalse(getQueryRunner().tableExists(getSession(), "test_create"));

        assertQueryFails("CREATE TABLE test_create (a bad_type)", ".* Unknown type 'bad_type' for column 'a'");
        assertFalse(getQueryRunner().tableExists(getSession(), "test_create"));

        // Replace test_create_table_if_not_exists with test_create_table_if_not_exist to fetch max size naming on oracle
        assertUpdate("CREATE TABLE test_create_table_if_not_exist (a bigint, b varchar, c double)");
        assertTrue(getQueryRunner().tableExists(getSession(), "test_create_table_if_not_exist"));
        assertTableColumnNames("test_create_table_if_not_exist", "a", "b", "c");

        assertUpdate("CREATE TABLE IF NOT EXISTS test_create_table_if_not_exist (d bigint, e varchar)");
        assertTrue(getQueryRunner().tableExists(getSession(), "test_create_table_if_not_exist"));
        assertTableColumnNames("test_create_table_if_not_exist", "a", "b", "c");

        assertUpdate("DROP TABLE test_create_table_if_not_exist");
        assertFalse(getQueryRunner().tableExists(getSession(), "test_create_table_if_not_exist"));

        // Test CREATE TABLE LIKE
        assertUpdate("CREATE TABLE test_create_original (a bigint, b double, c varchar)");
        assertTrue(getQueryRunner().tableExists(getSession(), "test_create_original"));
        assertTableColumnNames("test_create_original", "a", "b", "c");

        assertUpdate("CREATE TABLE test_create_like (LIKE test_create_original, d boolean, e varchar)");
        assertTrue(getQueryRunner().tableExists(getSession(), "test_create_like"));
        assertTableColumnNames("test_create_like", "a", "b", "c", "d", "e");

        assertUpdate("DROP TABLE test_create_original");
        assertFalse(getQueryRunner().tableExists(getSession(), "test_create_original"));

        assertUpdate("DROP TABLE test_create_like");
        assertFalse(getQueryRunner().tableExists(getSession(), "test_create_like"));
    }

    @Test
    @Override
    public void testShowColumns()
    {
        MaterializedResult actual = computeActual("SHOW COLUMNS FROM orders");

        MaterializedResult expectedParametrizedVarchar = resultBuilder(getSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .row("orderkey", "bigint", "", "")
                .row("custkey", "bigint", "", "")
                .row("orderstatus", "varchar(1)", "", "")
                .row("totalprice", "double", "", "")
                .row("orderdate", "timestamp", "", "")
                .row("orderpriority", "varchar(15)", "", "")
                .row("clerk", "varchar(15)", "", "")
                .row("shippriority", "bigint", "", "")
                .row("comment", "varchar(79)", "", "")
                .build();

        // Until we migrate all connectors to parametrized varchar we check two options
        assertTrue(actual.equals(expectedParametrizedVarchar),
                format("%s does not matches %s", actual, expectedParametrizedVarchar));
    }

    @Test
    @Override
    public void testInsertWithCoercion()
    {
        assertUpdate("CREATE TABLE test_insert_with_coercion (" +
                "tinyint_column TINYINT, " +
                "integer_column INTEGER, " +
                "decimal_column DECIMAL(5, 3), " +
                "real_column REAL, " +
                "char_column CHAR(3), " +
                "bounded_varchar_column VARCHAR(3), " +
                "unbounded_varchar_column VARCHAR, " +
                "date_column DATE)");

        assertUpdate("INSERT INTO test_insert_with_coercion (tinyint_column, integer_column, decimal_column, real_column) VALUES (1e0, 2e0, 3e0, 4e0)", 1);
        assertUpdate("INSERT INTO test_insert_with_coercion (char_column, bounded_varchar_column, unbounded_varchar_column) VALUES (CAST('aa     ' AS varchar), CAST('aa     ' AS varchar), CAST('aa     ' AS varchar))", 1);
        assertUpdate("INSERT INTO test_insert_with_coercion (char_column, bounded_varchar_column, unbounded_varchar_column) VALUES (NULL, NULL, NULL)", 1);
        assertUpdate("INSERT INTO test_insert_with_coercion (char_column, bounded_varchar_column, unbounded_varchar_column) VALUES (CAST(NULL AS varchar), CAST(NULL AS varchar), CAST(NULL AS varchar))", 1);
        assertUpdate("INSERT INTO test_insert_with_coercion (date_column) VALUES (TIMESTAMP '2019-11-18 22:13:40')", 1);

        // at oracle date field has time part to
        assertQuery(
                "SELECT * FROM test_insert_with_coercion",
                "VALUES " +
                        "(1, 2, 3, 4, NULL, NULL, NULL, NULL), " +
                        "(NULL, NULL, NULL, NULL, 'aa ', 'aa ', 'aa     ', NULL), " +
                        "(NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL), " +
                        "(NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL), " +
                        "(NULL, NULL, NULL, NULL, NULL, NULL, NULL, TIMESTAMP '2019-11-18 22:13:40')");

        // this wont fail
        //assertQueryFails("INSERT INTO test_insert_with_coercion (integer_column) VALUES (3e9)", "Out of range for integer: 3.0E9");
        assertQueryFails("INSERT INTO test_insert_with_coercion (char_column) VALUES ('abcd')", "Cannot truncate non-space characters on INSERT");
        assertQueryFails("INSERT INTO test_insert_with_coercion (bounded_varchar_column) VALUES ('abcd')", "Cannot truncate non-space characters on INSERT");

        assertUpdate("DROP TABLE test_insert_with_coercion");
    }

    @Test
    @Override
    // as all number formats are saved as NUMERIC, when reading the value this will be bigint or bigdecimal
    // all integer values are converted to long
    public void testAddColumn()
    {
        assertUpdate("CREATE TABLE test_add_column AS SELECT 123 x", 1);
        assertUpdate("CREATE TABLE test_add_column_a AS SELECT 234 x, 111 a", 1);
        assertUpdate("CREATE TABLE test_add_column_ab AS SELECT 345 x, 222 a, 33.3E0 b", 1);

        assertQueryFails("ALTER TABLE test_add_column ADD COLUMN x bigint", ".* Column 'x' already exists");
        assertQueryFails("ALTER TABLE test_add_column ADD COLUMN X bigint", ".* Column 'X' already exists");
        assertQueryFails("ALTER TABLE test_add_column ADD COLUMN q bad_type", ".* Unknown type 'bad_type' for column 'q'");

        assertUpdate("ALTER TABLE test_add_column ADD COLUMN a bigint");
        assertUpdate("INSERT INTO test_add_column SELECT * FROM test_add_column_a", 1);
        MaterializedResult materializedRows = computeActual("SELECT x, a FROM test_add_column ORDER BY x");
        assertEquals(materializedRows.getMaterializedRows().get(0).getField(0), 123L);
        assertNull(materializedRows.getMaterializedRows().get(0).getField(1));
        assertEquals(materializedRows.getMaterializedRows().get(1).getField(0), 234L);
        assertEquals(materializedRows.getMaterializedRows().get(1).getField(1), 111L);

        assertUpdate("ALTER TABLE test_add_column ADD COLUMN b double");
        assertUpdate("INSERT INTO test_add_column SELECT * FROM test_add_column_ab", 1);
        materializedRows = computeActual("SELECT x, a, b FROM test_add_column ORDER BY x");
        assertEquals(materializedRows.getMaterializedRows().get(0).getField(0), 123L);
        assertNull(materializedRows.getMaterializedRows().get(0).getField(1));
        assertNull(materializedRows.getMaterializedRows().get(0).getField(2));
        assertEquals(materializedRows.getMaterializedRows().get(1).getField(0), 234L);
        assertEquals(materializedRows.getMaterializedRows().get(1).getField(1), 111L);
        assertNull(materializedRows.getMaterializedRows().get(1).getField(2));
        assertEquals(materializedRows.getMaterializedRows().get(2).getField(0), 345L);
        assertEquals(materializedRows.getMaterializedRows().get(2).getField(1), 222L);
        assertEquals(materializedRows.getMaterializedRows().get(2).getField(2), 33.3);

        assertUpdate("DROP TABLE test_add_column");
        assertUpdate("DROP TABLE test_add_column_a");
        assertUpdate("DROP TABLE test_add_column_ab");
        assertFalse(getQueryRunner().tableExists(getSession(), "test_add_column"));
        assertFalse(getQueryRunner().tableExists(getSession(), "test_add_column_a"));
        assertFalse(getQueryRunner().tableExists(getSession(), "test_add_column_ab"));
    }

    @Test
    @Override
    // as all number formats are saved as NUMERIC, when reading the value this will be bigint or bigdecimal
    // all integer values are converted to long
    public void testRenameColumn()
    {
        assertUpdate("CREATE TABLE test_rename_column AS SELECT 123 x", 1);

        assertUpdate("ALTER TABLE test_rename_column RENAME COLUMN x TO y");
        MaterializedResult materializedRows = computeActual("SELECT y FROM test_rename_column");
        assertEquals(getOnlyElement(materializedRows.getMaterializedRows()).getField(0), 123L);

        assertUpdate("ALTER TABLE test_rename_column RENAME COLUMN y TO Z");
        materializedRows = computeActual("SELECT z FROM test_rename_column");
        assertEquals(getOnlyElement(materializedRows.getMaterializedRows()).getField(0), 123L);

        assertUpdate("DROP TABLE test_rename_column");
        assertFalse(getQueryRunner().tableExists(getSession(), "test_rename_column"));
    }

    @Test
    @Override
    // as all number formats are saved as NUMERIC, when reading the value this will be bigint or bigdecimal
    // all integer values are converted to long
    public void testRenameTable()
    {
        assertUpdate("CREATE TABLE test_rename AS SELECT 123 x", 1);

        assertUpdate("ALTER TABLE test_rename RENAME TO test_rename_new");
        MaterializedResult materializedRows = computeActual("SELECT x FROM test_rename_new");
        assertEquals(getOnlyElement(materializedRows.getMaterializedRows()).getField(0), 123L);

        // provide new table name in uppercase
        assertUpdate("ALTER TABLE test_rename_new RENAME TO TEST_RENAME");
        materializedRows = computeActual("SELECT x FROM test_rename");
        assertEquals(getOnlyElement(materializedRows.getMaterializedRows()).getField(0), 123L);

        assertUpdate("DROP TABLE test_rename");

        assertFalse(getQueryRunner().tableExists(getSession(), "test_rename"));
        assertFalse(getQueryRunner().tableExists(getSession(), "test_rename_new"));
    }
}
