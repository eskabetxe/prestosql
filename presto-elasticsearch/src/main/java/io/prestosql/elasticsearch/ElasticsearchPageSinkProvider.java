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
package io.prestosql.elasticsearch;

import io.prestosql.elasticsearch.client.ElasticsearchClient;
import io.prestosql.spi.connector.ConnectorInsertTableHandle;
import io.prestosql.spi.connector.ConnectorOutputTableHandle;
import io.prestosql.spi.connector.ConnectorPageSink;
import io.prestosql.spi.connector.ConnectorPageSinkProvider;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.type.TypeManager;

import javax.inject.Inject;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class ElasticsearchPageSinkProvider
        implements ConnectorPageSinkProvider
{
    private final ElasticsearchClient clientSession;

    @Inject
    public ElasticsearchPageSinkProvider(ElasticsearchClient clientSession)
    {
        this.clientSession = requireNonNull(clientSession, "clientSession is null");
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorOutputTableHandle outputTableHandle)
    {
        requireNonNull(outputTableHandle, "outputTableHandle is null");
        checkArgument(outputTableHandle instanceof ElasticsearchOutputTableHandle, "outputTableHandle is not an instance of ElasticsearchOutputTableHandle");
        ElasticsearchOutputTableHandle handle = (ElasticsearchOutputTableHandle) outputTableHandle;
        return new ElasticsearchPageSink(
                session,
                clientSession,
                handle.getIndex(),
                handle.getColumnNames(),
                handle.getColumnTypes());
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorInsertTableHandle insertTableHandle)
    {
        requireNonNull(insertTableHandle, "insertTableHandle is null");
        checkArgument(insertTableHandle instanceof ElasticsearchInsertTableHandle, "insertTableHandle is not an instance of ElasticsearchInsertTableHandle");
        ElasticsearchInsertTableHandle handle = (ElasticsearchInsertTableHandle) insertTableHandle;

        return new ElasticsearchPageSink(
                session,
                clientSession,
                handle.getIndex(),
                handle.getColumnNames(),
                handle.getColumnTypes());
    }
}
