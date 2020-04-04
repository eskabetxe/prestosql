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

import io.prestosql.plugin.jdbc.BaseJdbcConfig;
import io.prestosql.plugin.jdbc.ConnectionFactory;
import io.prestosql.plugin.jdbc.JdbcIdentity;
import io.prestosql.plugin.jdbc.credential.CredentialProvider;
import oracle.jdbc.OracleConnection;
import oracle.jdbc.OracleDriver;
import oracle.ucp.jdbc.PoolDataSource;
import oracle.ucp.jdbc.PoolDataSourceFactory;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.Optional;
import java.util.Properties;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class OracleConnectorFactory
        implements ConnectionFactory
{
    private final String connectionUrl;
    private final boolean connectionPoolEnabled;
    private final int connectionPoolMinSize;
    private final int connectionPoolMaxSize;
    private final Properties connectionProperties;
    private final CredentialProvider credentialProvider;

    private PoolDataSource pds;
    private Driver driver;

    public OracleConnectorFactory(
            BaseJdbcConfig config,
            OracleConfig oracleConfig,
            CredentialProvider credentialProvider)
            throws SQLException
    {
        this.connectionUrl = requireNonNull(config.getConnectionUrl(), "credentialProvider is null");
        this.connectionPoolEnabled = oracleConfig.isConnectionPoolEnabled();
        this.connectionPoolMinSize = oracleConfig.getConnectionPoolMinSize();
        this.connectionPoolMaxSize = oracleConfig.getConnectionPoolMaxSize();

        this.credentialProvider = requireNonNull(credentialProvider, "credentialProvider is null");

        this.connectionProperties = new Properties();
        this.connectionProperties.setProperty(OracleConnection.CONNECTION_PROPERTY_INCLUDE_SYNONYMS, String.valueOf(oracleConfig.isSynonymsEnabled()));

        if (connectionPoolEnabled) {
            this.startConnectionPool();
        }
        else {
            this.startDriver();
        }
    }

    @Override
    public Connection openConnection(JdbcIdentity identity)
            throws SQLException
    {
        Optional<String> user = credentialProvider.getConnectionUser(Optional.of(identity));
        Optional<String> password = credentialProvider.getConnectionPassword(Optional.of(identity));

        checkState(user.isPresent(), "Credentials returned null user");
        checkState(password.isPresent(), "Credentials returned null password");

        if (connectionPoolEnabled) {
            return pds.getConnection(user.get(), password.get());
        }
        else {
            Properties properties = new Properties();
            properties.putAll(connectionProperties);
            properties.put("user", user.get());
            properties.put("password", password.get());
            return driver.connect(connectionUrl, properties);
        }
    }

    private void startConnectionPool()
            throws SQLException
    {
        pds = PoolDataSourceFactory.getPoolDataSource();

        //Setting connection properties of the data source
        pds.setConnectionFactoryClassName("oracle.jdbc.pool.OracleDataSource");
        pds.setURL(connectionUrl);

        //Setting pool properties
        pds.setInitialPoolSize(connectionPoolMinSize);
        pds.setMinPoolSize(connectionPoolMinSize);
        pds.setMaxPoolSize(connectionPoolMaxSize);
        pds.setConnectionProperties(connectionProperties);
    }

    private void startDriver()
    {
        driver = new OracleDriver();
    }
}
