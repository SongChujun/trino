package io.trino.operator;

import io.trino.spi.Page;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.Optional;

public class IncrementalJoinBridge
{
    // use buildside and probeside to distinguish between two hashtables
    private HashBuildAndProbeTable buildSidehashBuildAndProbeTable;
    private HashBuildAndProbeTable probeSidehashBuildAndProbeTable;

    public  IncrementalJoinBridge(
            PagesHashStrategy pagesHashStrategy,
            int expectedPositions,
            PositionLinks.FactoryBuilder positionLinks,
            JoinProbe.JoinProbeFactory joinProbeFactory,
            List<Type> buildOutputTypes,
            Optional<JoinFilterFunction> filterFunction,
            LookupJoinOperators.JoinType joinType,
            boolean outputSingleMatch,
            boolean eagerCompact)
    {
        //subject to change
        buildSidehashBuildAndProbeTable = new HashBuildAndProbeTable(pagesHashStrategy,expectedPositions,
            positionLinks,joinProbeFactory,buildOutputTypes,filterFunction,
            joinType,outputSingleMatch,eagerCompact);

        probeSidehashBuildAndProbeTable = new HashBuildAndProbeTable(pagesHashStrategy,expectedPositions,
                positionLinks,joinProbeFactory,buildOutputTypes,filterFunction,
                joinType,outputSingleMatch,eagerCompact);


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
        return joinedPage
    }

    public Page buildSideProbe(Page page) {
        return buildSidehashBuildAndProbeTable.joinPage(page);
    }

    public void buildSideInsert(Page page) {
        buildSidehashBuildAndProbeTable.addPage(page);

    }

    public Page probeSideProbe(Page page) {
        return probeSidehashBuildAndProbeTable.joinPage(page);
    }

    public void probeSideInsert(Page page) {
        probeSidehashBuildAndProbeTable.addPage(page);
    }

}
