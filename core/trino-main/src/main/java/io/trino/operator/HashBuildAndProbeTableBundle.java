package io.trino.operator;

import io.trino.spi.Page;
import io.trino.spi.type.Type;
import io.trino.sql.gen.JoinFilterFunctionCompiler;
import io.trino.type.BlockTypeOperators;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

public class HashBuildAndProbeTableBundle
{
    // use buildside and probeside to distinguish between two hashtables
    private HashBuildAndProbeTable buildSidehashBuildAndProbeTable;
    private HashBuildAndProbeTable probeSidehashBuildAndProbeTable;

    public  HashBuildAndProbeTableBundle(
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
            boolean eagerCompact
    )
    {
        //subject to change
        JoinProbe.JoinProbeFactory buildJoinProbeFactory =  new JoinProbe.JoinProbeFactory(buildOutputChannels.get().stream().mapToInt(i -> i).toArray(), buildJoinChannels, buildHashChannel);
        JoinProbe.JoinProbeFactory probeJoinProbeFactory =  new JoinProbe.JoinProbeFactory(probeOutputChannels.get().stream().mapToInt(i -> i).toArray(), probeJoinChannels, probeHashChannel);
        buildSidehashBuildAndProbeTable = new HashBuildAndProbeTable(buildTypes,buildHashChannel,buildJoinChannels,buildOutputTypes,
                    blockTypeOperators,expectedPositions,buildJoinProbeFactory,joinType,outputSingleMatch,eagerCompact);
        probeSidehashBuildAndProbeTable = new HashBuildAndProbeTable(probeTypes,probeHashChannel,probeJoinChannels,probeOutputTypes,
                    blockTypeOperators,expectedPositions,probeJoinProbeFactory, joinType,outputSingleMatch,eagerCompact);
    }

    public HashBuildAndProbeTable getBuildSideHashTable()
    {

        return buildSidehashBuildAndProbeTable;
    }

    public HashBuildAndProbeTable getProbeSideHashTable() {
        return probeSidehashBuildAndProbeTable;
    }

    // first process buildSide the process probeside to avoid deadlock
    public Page processBuildSidePage(Page page) {
        buildSidehashBuildAndProbeTable.addPage(page);
        return probeSidehashBuildAndProbeTable.joinPage(page);
    }

    //first process buildSide the process probeside to avoid deadlock
    public Page processProbeSidePage(Page page) {
        Page joinedPage = buildSidehashBuildAndProbeTable.joinPage(page);
        probeSidehashBuildAndProbeTable.addPage(page);
        return joinedPage;
    }

}
