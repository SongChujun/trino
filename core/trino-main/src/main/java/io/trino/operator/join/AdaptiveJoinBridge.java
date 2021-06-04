package io.trino.operator.join;

import io.trino.operator.join.HashBuildAndProbeTable;
import io.trino.spi.Page;
import io.trino.spi.type.Type;
import io.trino.sql.gen.JoinFilterFunctionCompiler;
import io.trino.type.BlockTypeOperators;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

public class AdaptiveJoinBridge
{
    // use buildside and probeside to distinguish between two hashtables
    private HashBuildAndProbeTable[] tables;

    public AdaptiveJoinBridge(
            List<Type> buildTypes,
            OptionalInt buildHashChannel,
            List<Integer> buildJoinChannels,
            List<Type> buildOutputTypes,
            Optional<List<Integer>> buildOutputChannels,
            BlockTypeOperators blockTypeOperators,
            int expectedPositions,
            LookupJoinOperatorFactory.JoinType joinType,
            boolean outputSingleMatch,
            boolean eagerCompact,
            int tableInstanceCount
    )
    {

        tables = new HashBuildAndProbeTable[tableInstanceCount];
        JoinProbe.JoinProbeFactory buildJoinProbeFactory =  new JoinProbe.JoinProbeFactory(buildOutputChannels.get().stream().mapToInt(i -> i).toArray(), buildJoinChannels, buildHashChannel);
        for (int i = 0; i<tableInstanceCount;i++) {
            tables[i] = new HashBuildAndProbeTable(buildTypes,buildHashChannel,
                    buildJoinChannels,buildOutputTypes,
                    blockTypeOperators, expectedPositions,buildJoinProbeFactory, joinType, outputSingleMatch, eagerCompact);
        }
    }

    public HashBuildAndProbeTable getHashTable(int i)
    {
        return tables[i];
    }

}
