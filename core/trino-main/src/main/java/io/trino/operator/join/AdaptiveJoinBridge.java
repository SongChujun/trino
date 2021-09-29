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
package io.trino.operator.join;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.trino.spi.type.Type;
import io.trino.type.BlockTypeOperators;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

public class AdaptiveJoinBridge
{
    // use buildside and probeside to distinguish between two hashtables
    private final HashBuildAndProbeTable[] tables;
    private int completedHashTableCnt;
    private SettableFuture<Boolean> hashBuildFinishedFuture;

    public AdaptiveJoinBridge(
            List<Type> buildTypes,
            OptionalInt buildHashChannel,
            OptionalInt probeHashChannel,
            List<Integer> buildJoinChannels,
            List<Integer> probeJoinChannels,
            List<Type> buildOutputTypes,
            Optional<List<Integer>> buildOutputChannels,
            Optional<List<Integer>> probeOutputChannels,
            BlockTypeOperators blockTypeOperators,
            int expectedPositions,
            LookupJoinOperatorFactory.JoinType joinType,
            boolean outputSingleMatch,
            boolean eagerCompact,
            int tableInstanceCount)
    {
        tables = new HashBuildAndProbeTable[tableInstanceCount];
        JoinProbe.JoinProbeFactory buildJoinProbeFactory = new JoinProbe.JoinProbeFactory(probeOutputChannels.get().stream().mapToInt(i -> i).toArray(), probeJoinChannels, probeHashChannel);
        for (int i = 0; i < tableInstanceCount; i++) {
            tables[i] = new HashBuildAndProbeTable(buildTypes, buildHashChannel,
                    buildJoinChannels, buildOutputTypes, buildOutputChannels,
                    blockTypeOperators, expectedPositions, buildJoinProbeFactory, joinType, outputSingleMatch, eagerCompact);
        }
        this.hashBuildFinishedFuture = SettableFuture.create();
        completedHashTableCnt = 0;
    }

    public HashBuildAndProbeTable getHashTable(int i)
    {
        return tables[i];
    }

    public List<LookupSource> getHashTables()
    {
        return Arrays.asList(tables);
    }

    public ListenableFuture<?> getBuildFinishedFuture()
    {
        return hashBuildFinishedFuture;
    }

    public synchronized void setBuildFinished(int i)
    {
        completedHashTableCnt += 1;
        if (completedHashTableCnt == tables.length) {
            hashBuildFinishedFuture.set(true);
        }
    }
}
