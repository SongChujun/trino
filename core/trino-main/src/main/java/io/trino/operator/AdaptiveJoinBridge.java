package io.trino.operator;

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
    private HashBuildAndProbeTableBundle[] tableBundles;

    public AdaptiveJoinBridge(
            List<Type> buildTypes,
            List<Type> probeTypes,
            OptionalInt buildHashChannel,
            OptionalInt probeHashChannel,
            List<Integer> buildJoinChannels,
            List<Integer> probeJoinChannels,
            List<Type> buildOutputTypes,
            List<Type> probeOutputTypes,
            Optional<List<Integer>> buildOutputChannels,
            Optional<List<Integer>> probeOutputChannels,
            BlockTypeOperators blockTypeOperators,
            int expectedPositions,
            LookupJoinOperators.JoinType joinType,
            boolean outputSingleMatch,
            boolean eagerCompact,
            int tableInstanceCount
    )
    {

        tableBundles = new HashBuildAndProbeTableBundle[tableInstanceCount];

        for (int i = 0; i<tableInstanceCount;i++) {
            tableBundles[i] = new HashBuildAndProbeTableBundle(buildTypes,probeTypes,buildHashChannel,probeHashChannel,
                     buildJoinChannels, probeJoinChannels,buildOutputTypes, probeOutputTypes, buildOutputChannels,probeOutputChannels,
                    blockTypeOperators, expectedPositions, joinType, outputSingleMatch, eagerCompact);
        }


    }

    public HashBuildAndProbeTableBundle getTableBundle(int i)
    {
        return tableBundles[i];
    }

}
