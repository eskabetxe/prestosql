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
import io.prestosql.Session;
import io.prestosql.plugin.tpch.TpchPlugin;
import io.prestosql.testing.DistributedQueryRunner;
import io.prestosql.testing.QueryRunner;
import io.prestosql.tpch.TpchTable;

import java.util.HashMap;
import java.util.Map;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.prestosql.plugin.oracle.OracleServer.TEST_PASS;
import static io.prestosql.plugin.oracle.OracleServer.TEST_SCHEMA;
import static io.prestosql.plugin.oracle.OracleServer.TEST_USER;
import static io.prestosql.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.prestosql.testing.QueryAssertions.copyTpchTables;
import static io.prestosql.testing.TestingSession.testSessionBuilder;

public final class OracleQueryRunner
{
    private OracleQueryRunner() {}

    public static QueryRunner createOracleQueryRunner(OracleServer server)
            throws Exception
    {
        return createOracleQueryRunner(server, ImmutableList.of());
    }

    public static QueryRunner createOracleQueryRunner(OracleServer server, Iterable<TpchTable<?>> tables)
            throws Exception
    {
        return createOracleQueryRunner(server, tables, false);
    }

    public static QueryRunner createOraclePoolQueryRunner(OracleServer server)
            throws Exception
    {
        return createOraclePoolQueryRunner(server, ImmutableList.of());
    }

    public static QueryRunner createOraclePoolQueryRunner(OracleServer server, Iterable<TpchTable<?>> tables)
            throws Exception
    {
        return createOracleQueryRunner(server, tables, true);
    }

    private static QueryRunner createOracleQueryRunner(OracleServer server, Iterable<TpchTable<?>> tables, boolean connectionPoolEnable)
            throws Exception
    {
        DistributedQueryRunner queryRunner = null;
        try {
            queryRunner = DistributedQueryRunner.builder(createSession()).build();

            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch");

            Map<String, String> connectorProperties = new HashMap<>();
            connectorProperties.putIfAbsent("connection-url", server.getJdbcUrl());
            connectorProperties.putIfAbsent("connection-user", TEST_USER);
            connectorProperties.putIfAbsent("connection-password", TEST_PASS);
            connectorProperties.putIfAbsent("allow-drop-table", "true");
            connectorProperties.putIfAbsent("oracle.connection-pool.enabled", String.valueOf(connectionPoolEnable));

            queryRunner.installPlugin(new OraclePlugin());
            queryRunner.createCatalog("oracle", "oracle", connectorProperties);

            copyTpchTables(queryRunner, "tpch", TINY_SCHEMA_NAME, createSession(), tables);

            return queryRunner;
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner, server);
            throw e;
        }
    }

    public static Session createSession()
    {
        return testSessionBuilder()
                .setCatalog("oracle")
                .setSchema(TEST_SCHEMA)
                .build();
    }
}
