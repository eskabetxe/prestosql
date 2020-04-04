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

import com.google.common.collect.ImmutableList;
import io.prestosql.testing.AbstractTestIntegrationSmokeTest;
import io.prestosql.testing.MaterializedResult;
import io.prestosql.testing.QueryRunner;
import io.prestosql.tpch.TpchTable;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.util.List;

import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.testing.assertions.Assert.assertEquals;
import static io.prestosql.tpch.TpchTable.ORDERS;

public class TestOracleIntegrationSmokeTest
        extends AbstractTestIntegrationSmokeTest
{
    private OracleServer oracleServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        oracleServer = new OracleServer();
        return createOracleQueryRunner(oracleServer, ImmutableList.of(ORDERS));
    }

    protected QueryRunner createOracleQueryRunner(OracleServer server, List<TpchTable<?>> tables)
            throws Exception
    {
        return OracleQueryRunner.createOracleQueryRunner(server, tables);
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
    {
        oracleServer.close();
    }

    @Test
    @Override
    public void testDescribeTable()
    {
        MaterializedResult expectedColumns = MaterializedResult.resultBuilder(getQueryRunner().getDefaultSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
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
        MaterializedResult actualColumns = computeActual("DESCRIBE orders");
        assertEquals(actualColumns, expectedColumns);
    }

    @Test
    @Override
    public void testSelectInformationSchemaColumns()
    {
        // avoid
    }
}
