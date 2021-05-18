package io.trino.operator;

import io.trino.spi.type.Type;

import java.util.List;
import java.util.Optional;

public class IncrementalJoinBridge
{
    private HashBuildAndProbeTable hashBuildAndProbeTable;
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
        hashBuildAndProbeTable = new HashBuildAndProbeTable(pagesHashStrategy,expectedPositions,
            positionLinks,joinProbeFactory,buildOutputTypes,filterFunction,
            joinType,outputSingleMatch,eagerCompact);
    }

    public HashBuildAndProbeTable getHashTable()
    {
        return hashBuildAndProbeTable;
    }
}
