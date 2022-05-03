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
package io.trino.split;

import io.trino.Session;
import io.trino.connector.CatalogName;
import io.trino.execution.QueryManagerConfig;
import io.trino.metadata.Metadata;
import io.trino.metadata.TableHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitManager.SplitSchedulingStrategy;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableLayoutHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.FixedSplitSource;

import javax.inject.Inject;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class SplitManager
{
    private final ConcurrentMap<CatalogName, ConnectorSplitManager> splitManagers = new ConcurrentHashMap<>();
    private final int minScheduleSplitBatchSize;
    private Map<TableHandle, TableHandle> colocateTableHandle;
    private Map<TableHandle, ConnectorSplitSource> colocateSplitSource;

    private Optional<Double> pushDownRatio = Optional.empty();

    private SplitAssignmentPolicy splitAssignmentPolicy = SplitAssignmentPolicy.NONE;

    public enum SplitAssignmentPolicy {
        NONE,
        STATIC,
        DYNAMIC
    }

    // NOTE: This only used for filling in the table layout if none is present by the time we
    // get splits. DO NOT USE IT FOR ANY OTHER PURPOSE, as it will be removed once table layouts
    // are gone entirely
    private final Metadata metadata;

    @Inject
    public SplitManager(QueryManagerConfig config, Metadata metadata)
    {
        this.minScheduleSplitBatchSize = config.getMinScheduleSplitBatchSize();
        this.metadata = metadata;
        this.colocateTableHandle = new HashMap<>();
        this.colocateSplitSource = new HashMap<>();
    }

    public void addConnectorSplitManager(CatalogName catalogName, ConnectorSplitManager connectorSplitManager)
    {
        requireNonNull(catalogName, "catalogName is null");
        requireNonNull(connectorSplitManager, "connectorSplitManager is null");
        checkState(splitManagers.putIfAbsent(catalogName, connectorSplitManager) == null, "SplitManager for connector '%s' is already registered", catalogName);
    }

    public void removeConnectorSplitManager(CatalogName catalogName)
    {
        splitManagers.remove(catalogName);
    }

    public SplitSource getSplits(Session session, TableHandle table, SplitSchedulingStrategy splitSchedulingStrategy, DynamicFilter dynamicFilter)
    {
        CatalogName catalogName = table.getCatalogName();
        ConnectorSplitManager splitManager = getConnectorSplitManager(catalogName);

        ConnectorSession connectorSession = session.toConnectorSession(catalogName);

        ConnectorSplitSource source;
        if (metadata.usesLegacyTableLayouts(session, table)) {
            ConnectorTableLayoutHandle layout = table.getLayout()
                    .orElseGet(() -> metadata.getLayout(session, table, Constraint.alwaysTrue(), Optional.empty())
                            .get()
                            .getNewTableHandle()
                            .getLayout().get());

            source = splitManager.getSplits(table.getTransaction(), connectorSession, layout, splitSchedulingStrategy);
        }
        else {
            if (colocateTableHandle.containsKey(table) && colocateSplitSource.containsKey(colocateTableHandle.get(table))) {
                TableHandle otherTableHandle = colocateTableHandle.get(table);
                if (splitAssignmentPolicy.equals(SplitAssignmentPolicy.DYNAMIC)) {
                    source = colocateSplitSource.get(otherTableHandle);
                }
                else if (splitAssignmentPolicy.equals(SplitAssignmentPolicy.STATIC)) {
                    source = colocateSplitSource.get(otherTableHandle);
                    if (source instanceof FixedSplitSource) {
                        source = ((FixedSplitSource) source).split(pushDownRatio.get());
                    }
                    else {
                        throw new UnsupportedOperationException();
                    }
                }
                else {
                    throw new IllegalStateException();
                }
                colocateTableHandle.remove(table);
                colocateTableHandle.remove(otherTableHandle);
                colocateSplitSource.remove(otherTableHandle);
            }
            else {
                source = splitManager.getSplits(
                        table.getTransaction(),
                        connectorSession,
                        table.getConnectorHandle(),
                        splitSchedulingStrategy,
                        dynamicFilter);
                if (colocateTableHandle.containsKey(table)) {
                    colocateSplitSource.put(table, source);
                }
            }
        }

        SplitSource splitSource = new ConnectorAwareSplitSource(catalogName, source);
        if (minScheduleSplitBatchSize > 1) {
            splitSource = new BufferingSplitSource(splitSource, minScheduleSplitBatchSize);
        }
        return splitSource;
    }

    private ConnectorSplitManager getConnectorSplitManager(CatalogName catalogName)
    {
        ConnectorSplitManager result = splitManagers.get(catalogName);
        checkArgument(result != null, "No split manager for connector '%s'", catalogName);
        return result;
    }

    public void registerColocateTableHandle(TableHandle th1, TableHandle th2)
    {
        this.colocateTableHandle.put(th1, th2);
        this.colocateTableHandle.put(th2, th1);
    }

    public void setPushDownRatio(double pushDownRatio)
    {
        this.pushDownRatio = Optional.of(clipRatio(pushDownRatio));
    }

    public void setSplitAssignmentPolicy(SplitAssignmentPolicy splitAssignmentPolicy)
    {
        this.splitAssignmentPolicy = splitAssignmentPolicy;
    }

    private double clipRatio(double pushDownRatio)
    {
        if (pushDownRatio < 0) {
            return 0;
        }
        else if (pushDownRatio > 1) {
            return 1;
        }
        return pushDownRatio;
    }

    public void replaceTableHandle(TableHandle oldTableHandle, TableHandle newTableHandle)
    {
        if (this.colocateTableHandle.containsKey(oldTableHandle)) {
            TableHandle otherTableHandle = colocateTableHandle.get(oldTableHandle);
            colocateTableHandle.remove(oldTableHandle);
            colocateTableHandle.remove(otherTableHandle);
            colocateTableHandle.put(newTableHandle, otherTableHandle);
            colocateTableHandle.put(otherTableHandle, newTableHandle);
        }
    }
}
