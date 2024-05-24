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
import io.trino.execution.SqlStageExecution;
import io.trino.execution.StageId;
import io.trino.metadata.InternalNode;
import io.trino.metadata.Metadata;
import io.trino.metadata.Split;
import io.trino.server.remotetask.HttpRemoteTask;
import io.trino.spi.Page;
import io.trino.split.EmptySplit;
import io.trino.sql.analyzer.FeaturesConfig;
import io.trino.sql.planner.plan.PlanNode;
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

public class DynamicJoinPushdownService
{
    public static int schedulingInterval;

    public static FeaturesConfig.ElasticJoinType elasticJoinType;

    private static double dbNodePressureThreshold;

    private static FeaturesConfig.DefaultDynamicJoinPushdownDirection defaultDynamicJoinPushdownDirection;

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

        private boolean checkContainSortMergeAdaptiveJoinNode(PlanNode root)
        {
            if (root instanceof SortMergeAdaptiveJoinNode) {
                return true;
            }
            for (PlanNode source : root.getSources()) {
                boolean subRes = checkContainSortMergeAdaptiveJoinNode(source);
                if (subRes) {
                    return true;
                }
            }
            return false;
        }

        public List<String> acknowledge(NodeTaskMap nodeTaskMap)
        {
            List<String> currentRoundFinishedSplits = new ArrayList<>();
            nodeSplitMap.forEach((node, splits) -> {
                Set<RemoteTask> remoteTasks = nodeTaskMap.getNodeTasks(node).getRemoteTasks();
                List<Page.SplitIdentifier> finishedSplits = remoteTasks.stream().filter(task -> (task instanceof HttpRemoteTask) && checkContainSortMergeAdaptiveJoinNode(((HttpRemoteTask) task).getPlanFragment().getRoot()))
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

    private Optional<StageId> winnerStageId = Optional.empty();

    private long scheduledSplits;

    private boolean noMoreSplits;

    private StageId upStageId;

    private StageId downStageId;

    private final Map<StageId, SplitsTracker> stageSplitsTracker = new HashMap<>();

    private Map<StageId, Set<String>> stageAcknowledgedSplits = new HashMap<>();

    private int schedulerRoundNumber;

    private Metadata metadata;

    private boolean allSplitsFromThisRoundScheduled;

    private boolean enabled;

    public DynamicJoinPushdownService(Metadata metadata, Boolean enabled)
    {
        this.metadata = metadata;
        this.enabled = enabled;
    }

    public DynamicJoinPushdownService(Metadata metadata)
    {
        this.metadata = metadata;
        this.enabled = true;
    }

    public static void setSchedulingParameters(FeaturesConfig.ElasticJoinType elasticJoinType, FeaturesConfig.DefaultDynamicJoinPushdownDirection defaultDynamicJoinPushdownDirection, int schedulingInterval, double dbNodePressureThreshold)
    {
        DynamicJoinPushdownService.elasticJoinType = elasticJoinType;
        DynamicJoinPushdownService.schedulingInterval = schedulingInterval;
        DynamicJoinPushdownService.dbNodePressureThreshold = dbNodePressureThreshold;
        DynamicJoinPushdownService.defaultDynamicJoinPushdownDirection = defaultDynamicJoinPushdownDirection;
    }

    public void setUpOrDownStageId(SqlStageExecution stage)
    {
        if ((!elasticJoinType.equals(FeaturesConfig.ElasticJoinType.DYNAMIC)) && (!elasticJoinType.equals(FeaturesConfig.ElasticJoinType.STATIC_MULTIPLE_SPLITS))) {
            return;
        }
        else {
            downStageId = stage.getStageId();
        }
        SqlStageExecution.StagePlacement placement = stage.getStagePlacement();
        if (placement == null) {
            return;
        }
        if (stage.getStagePlacement().equals(SqlStageExecution.StagePlacement.UP)) {
            this.upStageId = stage.getStageId();
        }
        else if (stage.getStagePlacement().equals(SqlStageExecution.StagePlacement.DOWN)) {
            this.downStageId = stage.getStageId();
        }
    }

    private void addScheduledSplitCount(long count)
    {
        this.scheduledSplits = this.scheduledSplits + count;
        if (this.scheduledSplits % schedulingInterval == 0) {
            allSplitsFromThisRoundScheduled = true;
            log.debug("Probe round started");
        }
    }

    public boolean dynamicExecutionEnabled()
    {
        return enabled & (elasticJoinType == FeaturesConfig.ElasticJoinType.DYNAMIC || elasticJoinType == FeaturesConfig.ElasticJoinType.STATIC_MULTIPLE_SPLITS);
    }

    public void setWinningStageId()
    {
        double dbNodeCPUPressure = metadata.getDbNodeCPUPressure();
        double workersCPUPressure = metadata.getWorkersCPUPressure().stream().mapToDouble(Double::doubleValue).average().orElse(0);
        if (dbNodeCPUPressure >= dbNodePressureThreshold && (workersCPUPressure < dbNodePressureThreshold)) {
            winnerStageId = Optional.of(upStageId);
        }
        else if (dbNodeCPUPressure < dbNodePressureThreshold && (workersCPUPressure >= dbNodePressureThreshold)) {
            winnerStageId = Optional.of(downStageId);
        }
        else {
            if (defaultDynamicJoinPushdownDirection.equals(FeaturesConfig.DefaultDynamicJoinPushdownDirection.UP)) {
                winnerStageId = Optional.of(upStageId);
            }
            else {
                winnerStageId = Optional.of(downStageId);
            }
        }
    }

    public boolean checkCouldSchedule(StageId stageId)
    {
        if (!dynamicExecutionEnabled()) {
            return true;
        }

        if (elasticJoinType == FeaturesConfig.ElasticJoinType.STATIC_MULTIPLE_SPLITS) {
            return splitsFinished(stageId);
        }

        if (!allSplitsFromThisRoundScheduled) {
            return winnerStageId.isPresent() && winnerStageId.get().equals(stageId);
        }

        if (!allSplitsFromBothStageFinished()) {
            return false;
        }

        setWinningStageId();

        allSplitsFromThisRoundScheduled = false;
        if (winnerStageId.get().equals(stageId)) {
            log.debug("schedule scheduling split for stage %s", stageId);
            return true;
        }
        return false;
    }

    private boolean allSplitsFromBothStageFinished()
    {
        return stageSplitsTracker.values().stream().allMatch(SplitsTracker::allSplitsFinished);
    }

    private boolean splitsFinished(StageId stageId)
    {
        return !stageSplitsTracker.containsKey(stageId) || stageSplitsTracker.get(stageId).allSplitsFinished();
    }

    public int getSchedulingBatchSize()
    {
        return schedulingInterval;
    }

    public void scheduleSplits(StageId stageId, Multimap<InternalNode, Split> splitAssignment)
    {
        if (!dynamicExecutionEnabled()) {
            return;
        }
        if (splitAssignment.isEmpty()) {
            return;
        }

//        checkState(winnerStageId.isPresent() && winnerStageId.get().equals(stageId), "Winner stage %s not equal to stageId %s", winnerStageId.get(), stageId);
        stageSplitsTracker.computeIfAbsent(stageId, k -> new SplitsTracker()).add(splitAssignment);
        this.addScheduledSplitCount(splitAssignment.values().stream().filter(s -> !(s.getConnectorSplit() instanceof EmptySplit)).count());
    }

    public void acknowledge(StageId stageId, NodeTaskMap nodeTaskMap)
    {
        if (!dynamicExecutionEnabled()) {
            return;
        }
        List<String> acknowledgedSplits = stageSplitsTracker.computeIfAbsent(stageId, k -> new SplitsTracker()).acknowledge(nodeTaskMap);
        stageAcknowledgedSplits.computeIfAbsent(stageId, k -> new HashSet<>()).addAll(acknowledgedSplits);
    }

    public void setNoMoreSplits()
    {
        this.noMoreSplits = true;
    }

    public boolean isNoMoreSplits()
    {
        return dynamicExecutionEnabled() && this.noMoreSplits;
    }
}
