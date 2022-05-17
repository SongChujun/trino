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
package io.trino.server;

import com.google.common.collect.Multimap;
import io.airlift.log.Logger;
import io.trino.execution.NodeTaskMap;
import io.trino.execution.RemoteTask;
import io.trino.execution.StageId;
import io.trino.metadata.InternalNode;
import io.trino.metadata.Split;
import io.trino.server.remotetask.HttpRemoteTask;
import io.trino.spi.Page;
import io.trino.sql.analyzer.FeaturesConfig;
import io.trino.sql.planner.plan.SortMergeAdaptiveJoinNode;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkState;

public class DynamicJoinPushdownService
{
    public static int probeInterval;

    public static int probeBatchSize;

    public static int schedulingBatchSize;

    public static FeaturesConfig.ElasticJoinType elasticJoinType;
    private static final Logger log = Logger.get(DynamicJoinPushdownService.class);

    private static class SplitsTracker
    {
        private final Map<InternalNode, Set<String>> nodeSplitMap = new HashMap<>();

        Set<String> finishedSplitsRecording = new HashSet<>();

        private int finishedSplitsCnt;

        public void add(Multimap<InternalNode, Split> splitAssignment)
        {
            Map<String, Set<String>> scheduledSplitsRecording = new HashMap<>();
            splitAssignment.asMap().forEach((node, splits) -> {
                Set<String> splitIds = nodeSplitMap.computeIfAbsent(node, k -> new HashSet<>());
                splits.forEach(s -> {
                    String splitIdentifier = s.getConnectorSplit().getIdentifier();
                    if (!splitIdentifier.isEmpty()) {
                        splitIds.add(splitIdentifier);
                    }
                });
                Set<String> currentNodeSplitAssignment = scheduledSplitsRecording.computeIfAbsent(node.getNodeIdentifier(), k -> new HashSet<>());
                currentNodeSplitAssignment.addAll(splitIds);
            });
            if (!scheduledSplitsRecording.isEmpty()) {
                log.debug("Scheduled splits " + scheduledSplitsRecording + " on stage " + this.hashCode());
            }
        }

        public List<String> acknowledge(NodeTaskMap nodeTaskMap)
        {
            List<String> currentRoundFinishedSplits = new ArrayList<>();
            nodeSplitMap.forEach((node, splits) -> {
                Set<RemoteTask> remoteTasks = nodeTaskMap.getNodeTasks(node).getRemoteTasks();
                List<Page.SplitIdentifier> finishedSplits = remoteTasks.stream().filter(task -> (task instanceof HttpRemoteTask) && ((HttpRemoteTask) task).getPlanFragment().getRoot() instanceof SortMergeAdaptiveJoinNode)
                        .map(task -> task.getTaskStatus().getSplitFinishedPagesInfo().keySet()).flatMap(Collection::stream).collect(Collectors.toList());
                List<String> finishedSplitIds = finishedSplits.stream().map(s -> s.getTableName() + "_" + s.getId()).collect(Collectors.toList());
                finishedSplitsCnt += finishedSplitIds.size();
                splits.stream().filter(finishedSplitIds::contains).forEach(finishedSplitsRecording::add);
                splits.stream().filter(finishedSplitIds::contains).forEach(currentRoundFinishedSplits::add);
                splits.removeIf(finishedSplitIds::contains);
            });
            if (!finishedSplitsRecording.isEmpty() && allSplitsFinished()) {
                log.debug("All splits finished : " + finishedSplitsRecording + " on scheduler " + this.hashCode());
                finishedSplitsRecording.clear();
            }
            return currentRoundFinishedSplits;
        }

        public boolean allSplitsFinished()
        {
            return nodeSplitMap.values().stream().allMatch(Set::isEmpty);
        }

        public boolean hasSplitsFinished()
        {
            return finishedSplitsCnt > 0;
        }
    }

    public enum DynamicSchedulingState
    {
        PROBE,
        SCHEDULE,
    }

    private Optional<StageId> winnerStageId = Optional.empty();

    private int scheduledSplits;

    private boolean noMoreSplits;

    private DynamicSchedulingState dynamicSchedulingState = DynamicSchedulingState.PROBE;

    private final Map<StageId, SplitsTracker> stageSplitsTracker = new HashMap<>();

    private Map<StageId, List<String>> probeSplitsMap = new HashMap<>();

    private Map<StageId, Set<String>> stageAcknowledgedSplits = new HashMap<>();

    private int schedulerRoundNumber;

    private boolean allSplitsFromLastRoundFinished;

    private Map<StageId, Integer> stageRoundNumberMap = new HashMap<>();

    public DynamicJoinPushdownService()
    {
    }

    public static void setSchedulingParameters(FeaturesConfig.ElasticJoinType elasticJoinType, int probeInterval, int probeBatchSize, int schedulingBatchSize)
    {
        DynamicJoinPushdownService.probeInterval = probeInterval;
        DynamicJoinPushdownService.probeBatchSize = probeBatchSize;
        DynamicJoinPushdownService.elasticJoinType = elasticJoinType;
        DynamicJoinPushdownService.schedulingBatchSize = schedulingBatchSize;
    }

    private void addScheduledSplitCount(int count)
    {
        this.scheduledSplits = this.scheduledSplits + count;
        if (this.scheduledSplits % probeInterval == 0) {
            dynamicSchedulingState = DynamicSchedulingState.PROBE;
            allSplitsFromLastRoundFinished = false;
            log.debug("Probe round started");
        }
    }

    public boolean checkCouldSchedule(StageId stageId)
    {
        if (elasticJoinType != FeaturesConfig.ElasticJoinType.DYNAMIC) {
            return true;
        }

        int probeSplitScheduledCnt = probeSplitsMap.computeIfAbsent(stageId, k -> new ArrayList<>()).size();
        if ((dynamicSchedulingState == DynamicSchedulingState.PROBE) && (probeSplitScheduledCnt < probeBatchSize)) {
            if ((!allSplitsFromLastRoundFinished) && allSplitsFromBothStageFinished()) {
                allSplitsFromLastRoundFinished = true;
            }
            if (allSplitsFromLastRoundFinished) {
                log.debug("schedule probe split for stage %s", stageId);
                return true;
            }
        }
        if (dynamicSchedulingState == DynamicSchedulingState.SCHEDULE && winnerStageId.isPresent() && winnerStageId.get().equals(stageId)) {
            log.debug("schedule scheduling split for stage %s", stageId);
            return true;
        }
        return false;
    }

    private boolean allSplitsFromBothStageFinished()
    {
        return stageSplitsTracker.values().stream().allMatch(SplitsTracker::allSplitsFinished);
    }

    public int getSchedulingBatchSize()
    {
        if (dynamicSchedulingState == DynamicSchedulingState.PROBE) {
            return probeBatchSize;
        }
        else {
            return schedulingBatchSize;
        }
    }

    public int getProbeBatchSize()
    {
        return probeBatchSize;
    }

    public DynamicSchedulingState getDynamicSchedulingState()
    {
        return dynamicSchedulingState;
    }

    public void scheduleSplits(StageId stageId, Multimap<InternalNode, Split> splitAssignment)
    {
        if (splitAssignment.isEmpty()) {
            return;
        }
        if (dynamicSchedulingState == DynamicSchedulingState.PROBE) {
            int probeSplitScheduledCnt = probeSplitsMap.computeIfAbsent(stageId, k -> new ArrayList<>()).size();
            checkState(probeSplitScheduledCnt <= probeBatchSize, "Probe split scheduled for stage %s", stageId);
            checkState(splitAssignment.size() <= probeBatchSize, "Probe split batch size %s is not equal to probe batch size %s", splitAssignment.size(), probeBatchSize);
        }
        if (dynamicSchedulingState == DynamicSchedulingState.SCHEDULE) {
            checkState(winnerStageId.isPresent() && winnerStageId.get().equals(stageId), "Winner stage %s not equal to stageId %s", winnerStageId.get(), stageId);
        }
        stageSplitsTracker.computeIfAbsent(stageId, k -> new SplitsTracker()).add(splitAssignment);
        this.addScheduledSplitCount(splitAssignment.size());
    }

    public void acknowledge(StageId stageId, NodeTaskMap nodeTaskMap)
    {
        List<String> acknowledgedSplits = stageSplitsTracker.computeIfAbsent(stageId, k -> new SplitsTracker()).acknowledge(nodeTaskMap);
        stageAcknowledgedSplits.computeIfAbsent(stageId, k -> new HashSet<>()).addAll(acknowledgedSplits);
        if (!acknowledgedSplits.isEmpty() && !probeSplitsMap.get(stageId).isEmpty() && (dynamicSchedulingState == DynamicSchedulingState.PROBE)) {
            if (stageAcknowledgedSplits.get(stageId).containsAll(probeSplitsMap.get(stageId))) {
                stageAcknowledgedSplits.get(stageId).clear();
                probeSplitsMap.get(stageId).clear();
                int stageRoundNumber = stageRoundNumberMap.computeIfAbsent(stageId, k -> 0) + 1;
                stageRoundNumberMap.put(stageId, stageRoundNumber);
                if (stageRoundNumber > schedulerRoundNumber) {
                    log.debug("stage %s wins the probe", stageId);
                    schedulerRoundNumber = stageRoundNumber;
                    winnerStageId = Optional.of(stageId);
                    dynamicSchedulingState = DynamicSchedulingState.SCHEDULE;
                }
            }
        }
    }

    public void setProbeSplits(StageId stageId, List<Split> splits)
    {
        List<String> probeSplits = probeSplitsMap.computeIfAbsent(stageId, k -> new ArrayList<>());
        probeSplits.addAll(splits.stream().map(s -> s.getConnectorSplit().getIdentifier()).collect(Collectors.toList()));
    }

    public void setNoMoreSplits()
    {
        this.noMoreSplits = true;
    }

    public boolean isNoMoreSplits()
    {
        return (elasticJoinType == FeaturesConfig.ElasticJoinType.DYNAMIC) && this.noMoreSplits;
    }
}
