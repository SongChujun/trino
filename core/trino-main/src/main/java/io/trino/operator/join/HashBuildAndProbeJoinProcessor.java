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

import com.google.common.collect.ImmutableList;
import io.trino.spi.Page;
import io.trino.spi.type.Type;

import java.util.List;

import static io.trino.operator.join.LookupJoinOperatorFactory.JoinType.FULL_OUTER;
import static io.trino.operator.join.LookupJoinOperatorFactory.JoinType.PROBE_OUTER;

public class HashBuildAndProbeJoinProcessor
{
    private final JoinProbe.JoinProbeFactory joinProbeFactory;
    private final boolean probeOnOuterSide;
    private final JoinStatisticsCounter statisticsCounter;
    private final LookupJoinPageBuilder pageBuilder;
    private boolean currentProbePositionProducedRow;
    private long joinPosition = -1;
    private int joinSourcePositions;
    private final boolean outputSingleMatch;
    private JoinProbe probe;
    private final LookupSource partitionedLookupSource;

    private HashBuildAndProbeJoinProcessor(
            JoinProbe.JoinProbeFactory joinProbeFactory,
            LookupSource partitionedLookupSource,
            LookupJoinPageBuilder pageBuilder,
            LookupJoinOperatorFactory.JoinType joinType,
            boolean outputSingleMatch)
    {
        this.joinProbeFactory = joinProbeFactory;
        this.partitionedLookupSource = partitionedLookupSource;
        this.pageBuilder = pageBuilder;
        this.currentProbePositionProducedRow = false;
        this.outputSingleMatch = outputSingleMatch;
        this.statisticsCounter = new JoinStatisticsCounter(joinType);
        probeOnOuterSide = joinType == PROBE_OUTER || joinType == FULL_OUTER;
    }

    private Page joinPage()
    {
        do {
            if (probe.getPosition() >= 0) {
                if (!joinCurrentPosition(partitionedLookupSource)) {
                    break;
                }
                if (!currentProbePositionProducedRow) {
                    currentProbePositionProducedRow = true;
                    if (!outerJoinCurrentPosition()) {
                        break;
                    }
                }
                statisticsCounter.recordProbe(joinSourcePositions);
            }
            if (!advanceProbePosition(partitionedLookupSource)) {
                break;
            }
        }
        while (true);
        if (!pageBuilder.isEmpty()) {
            Page outputPage = pageBuilder.build(probe);
            pageBuilder.reset();
            return outputPage;
        }
        return null;
    }

    public List<Page> join(Page page)
    {
        ImmutableList.Builder<Page> res = new ImmutableList.Builder<>();
        probe = joinProbeFactory.createJoinProbe(page);
        while (!probe.isFinished()) {
            Page joinResult = joinPage();
            if (joinResult != null) {
                res.add(joinResult);
            }
        }
        return res.build();
    }

    public void reset()
    {
        this.joinPosition = -1;
        this.pageBuilder.reset();
        this.currentProbePositionProducedRow = false;
    }

    private boolean joinCurrentPosition(LookupSource lookupSource)
    {
        while (joinPosition >= 0) {
            if (lookupSource.isJoinPositionEligible(joinPosition, probe.getPosition(), probe.getPage())) {
                currentProbePositionProducedRow = true;

                pageBuilder.appendRow(probe, lookupSource, joinPosition);
                joinSourcePositions++;
            }

            if (outputSingleMatch && currentProbePositionProducedRow) {
                joinPosition = -1;
            }
            else {
                // get next position on lookup side for this probe row
                joinPosition = lookupSource.getNextJoinPosition(joinPosition, probe.getPosition(), probe.getPage());
            }

            if (pageBuilder.isFull()) {
                return false;
            }
        }
        return true;
    }

    private boolean outerJoinCurrentPosition()
    {
        if (probeOnOuterSide) {
            pageBuilder.appendNullForBuild(probe);
            return !pageBuilder.isFull();
        }
        return true;
    }

    private boolean advanceProbePosition(LookupSource lookupSource)
    {
        if (!probe.advanceNextPosition()) {
            return false;
        }

        // update join position
        joinPosition = probe.getCurrentJoinPosition(lookupSource);
        // reset row join state for next row
        joinSourcePositions = 0;
        currentProbePositionProducedRow = false;
        return true;
    }

    public static class HashBuildAndProbeJoinProcessorFactory
    {
        List<Type> buildOutputTypes;
        JoinProbe.JoinProbeFactory joinProbeFactory;
        LookupSource partitionedLookupSource;
        LookupJoinOperatorFactory.JoinType joinType;
        boolean outputSingleMatch;

        public HashBuildAndProbeJoinProcessorFactory(List<Type> buildOutputTypes,
                JoinProbe.JoinProbeFactory joinProbeFactory,
                LookupSource partitionedLookupSource,
                LookupJoinOperatorFactory.JoinType joinType,
                boolean outputSingleMatch)
        {
            this.buildOutputTypes = buildOutputTypes;
            this.joinProbeFactory = joinProbeFactory;
            this.partitionedLookupSource = partitionedLookupSource;
            this.outputSingleMatch = outputSingleMatch;
            this.joinType = joinType;
        }

        public HashBuildAndProbeJoinProcessor createHashBuildAndProbeJoinProcessor()
        {
            return new HashBuildAndProbeJoinProcessor(joinProbeFactory,
                    partitionedLookupSource, new LookupJoinPageBuilder(buildOutputTypes), joinType, outputSingleMatch);
        }
    }
}
