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
import io.trino.operator.DriverContext;
import io.trino.operator.Operator;
import io.trino.operator.OperatorContext;
import io.trino.operator.OperatorFactory;
import io.trino.operator.PartitionFunction;
import io.trino.spi.Page;
import io.trino.spi.block.IntArrayBlock;
import io.trino.spi.block.LongArrayBlock;
import io.trino.sql.planner.plan.PlanNodeId;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;

public class OuterJoinResultProcessingOperator
        implements Operator
{
    private final OperatorContext operatorContext;
    private final boolean isInnerJoin;
    private final Stack<Page> outputPageBuffer;
    private final List<Integer> leftPrimaryKeyChannels;
    private final List<Integer> leftJoinChannels;
    private final List<Integer> rightJoinChannels;
    private final List<Integer> outputChannels;
    private final Integer leftColumnsSize;
    private AdaptiveJoinBridge joinBridge;
    private final HashBuildAndProbeJoinProcessor joinProcessor;
    private boolean isFinished;
    private Map<Integer, Set<String>> duplicateSetMap;
    private PartitionFunction partitionFunction;
    private long extractTime;
    private long overallTime;
    String[] currentPrimaryKeyStr;

    public OuterJoinResultProcessingOperator(
            OperatorContext operatorContext,
            boolean isInnerJoin,
            List<Integer> leftPrimaryKeyChannels,
            List<Integer> leftJoinChannels,
            List<Integer> rightJoinChannels,
            List<Integer> outputChannels,
            Integer leftColumnsSize,
            AdaptiveJoinBridge joinBridge,
            HashBuildAndProbeJoinProcessor joinProcessor,
            Map<Integer, Set<String>> duplicateSetMap,
            PartitionFunction partitionFunction)
    {
        this.operatorContext = operatorContext;
        this.isInnerJoin = isInnerJoin;
        this.joinBridge = requireNonNull(joinBridge);
        this.joinProcessor = requireNonNull(joinProcessor);
        this.leftPrimaryKeyChannels = requireNonNull(leftPrimaryKeyChannels);
        this.leftJoinChannels = requireNonNull(leftJoinChannels);
        this.rightJoinChannels = requireNonNull(rightJoinChannels);
        this.outputChannels = requireNonNull(outputChannels);
        this.leftColumnsSize = requireNonNull(leftColumnsSize);
        this.outputPageBuffer = new Stack<>();
        this.duplicateSetMap = duplicateSetMap;
        this.partitionFunction = partitionFunction;
        this.extractTime = 0;
        this.overallTime = 0;
        this.currentPrimaryKeyStr = new String[partitionFunction.getPartitionCount()];
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        return joinBridge.getBuildFinishedFuture();
    }

    @Override
    public boolean needsInput()
    {
        return joinBridge.getBuildFinishedFuture().isDone();
    }

    @Override
    public void addInput(Page page)
    {
        long startTime = System.currentTimeMillis();
        Page[] extractedPages = extractPages(page, isInnerJoin);
        extractTime += System.currentTimeMillis() - startTime;
        List<Page> joinResult = joinProcessor.join(extractedPages[0]);
        overallTime += System.currentTimeMillis() - startTime;
        if (joinResult != null) {
            outputPageBuffer.addAll(joinResult);
        }
        outputPageBuffer.add(extractedPages[1]);
    }

    //specification: left: probeSide, right: outerSide
    //input format: probeSideData, outerSideData
    private Page[] extractPages(Page page, boolean isInnerJoin)
    {
        //nullIndicators encoding: 0 stands for null, t, 1 stands for s, null, 2 stands for s,t ,3 for null,null
        boolean[] nullIndicators = new boolean[page.getPositionCount()];
        boolean[] duplicateIndicators = new boolean[page.getPositionCount()];
        int[] leftRetainedPositions = new int[page.getPositionCount()];
        int[] outputRetainedPositions = new int[page.getPositionCount()];
        int leftRetainedPositionsPtr = 0;
        int outputRetainedPositionsPtr = 0;
        for (int i = 0; i < page.getPositionCount(); i++) {
            boolean rightNull = page.getBlock(rightJoinChannels.get(0)).isNull(i);
            String primaryKeyStr = tupleToString(page, leftPrimaryKeyChannels, i);
            int partition = partitionFunction.getPartition(page, i);
            if (primaryKeyStr.equals(currentPrimaryKeyStr[partition])) {
                duplicateIndicators[i] = true;
            }
            else {
                duplicateIndicators[i] = false;
                leftRetainedPositions[leftRetainedPositionsPtr] = i;
                leftRetainedPositionsPtr += 1;
                currentPrimaryKeyStr[partition] = primaryKeyStr;
            }
            if (rightNull) {
                nullIndicators[i] = true;
            }
            else {
                nullIndicators[i] = false;
                outputRetainedPositions[outputRetainedPositionsPtr] = i;
                outputRetainedPositionsPtr += 1;
            }
        }
        Page leftPage = page.copyPositions(leftRetainedPositions, 0, leftRetainedPositionsPtr).getColumns(Stream.concat(IntStream.range(0, leftColumnsSize).boxed(), Stream.of(page.getChannelCount() - 1)).mapToInt(i -> i).toArray());
        Page outputPage = page.copyPositions(outputRetainedPositions, 0, outputRetainedPositionsPtr).getColumns(outputChannels.stream().mapToInt(i -> i).toArray());
        return new Page[] {leftPage, outputPage};
    }

    private String tupleToString(Page page, List<Integer> channels, int row)
    {
        StringBuilder res = new StringBuilder();
        for (Integer channel : channels) {
            if (page.getBlock(channel) instanceof LongArrayBlock) {
                LongArrayBlock block = (LongArrayBlock) page.getBlock(channel);
                res.append(block.getLong(row, 0));
                res.append('*');
            }
            else if (page.getBlock(channel) instanceof IntArrayBlock) {
                IntArrayBlock block = (IntArrayBlock) page.getBlock(channel);
                res.append(block.getInt(row, 0));
                res.append('*');
            }
            else {
                verify(false);
            }
        }
        return res.toString();
    }

    @Override
    public Page getOutput()
    {
        if (outputPageBuffer.isEmpty()) {
            return null;
        }
        else {
            return outputPageBuffer.pop();
        }
    }

    @Override
    public boolean isFinished()
    {
        return isFinished && outputPageBuffer.isEmpty();
    }

    @Override
    public void finish()
    {
        isFinished = true;
    }

    public static class OuterJoinResultProcessingOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private AdaptiveJoinBridge joinBridge;
        private final HashBuildAndProbeJoinProcessor.HashBuildAndProbeJoinProcessorFactory joinProcessorFactory;
        private final boolean isInnerJoin;
        private final List<Integer> leftPrimaryKeyChannels;
        private final List<Integer> leftJoinChannels;
        private final List<Integer> rightJoinChannels;
        private final List<Integer> outputChannels;
        private final Integer leftColumnsSize;
        private final PartitionFunction partitionFunction;
        private final Map<Integer, Set<String>> duplicateSetMap;
        private boolean closed;

        public OuterJoinResultProcessingOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                AdaptiveJoinBridge joinBridge,
                HashBuildAndProbeJoinProcessor.HashBuildAndProbeJoinProcessorFactory joinProcessorFactory,
                boolean isInnerJoin,
                List<Integer> leftPrimaryKeyChannels,
                List<Integer> leftJoinChannels,
                List<Integer> rightJoinChannels,
                List<Integer> outputChannels,
                Integer leftColumnsSize,
                Integer partitioningCnt,
                PartitionFunction partitionFunction)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.joinBridge = joinBridge;
            this.joinProcessorFactory = requireNonNull(joinProcessorFactory, "lookupSourceFactoryManager is null");
            this.isInnerJoin = isInnerJoin;
            this.leftPrimaryKeyChannels = requireNonNull(leftPrimaryKeyChannels);
            this.leftJoinChannels = requireNonNull(leftJoinChannels);
            this.rightJoinChannels = requireNonNull(rightJoinChannels);
            this.outputChannels = requireNonNull(outputChannels);
            this.leftColumnsSize = requireNonNull(leftColumnsSize);
            this.duplicateSetMap = new HashMap<>();
            IntStream.range(0, partitioningCnt).forEach(i -> duplicateSetMap.put(i, ConcurrentHashMap.newKeySet()));
            this.partitionFunction = requireNonNull(partitionFunction);
        }

        @Override
        public OuterJoinResultProcessingOperator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, OuterJoinResultProcessingOperator.class.getSimpleName());
            Integer localPartitioningIndex = driverContext.getLocalPartitioningIndex();
            return new OuterJoinResultProcessingOperator(operatorContext, isInnerJoin,
                    leftPrimaryKeyChannels, leftJoinChannels, rightJoinChannels, outputChannels, leftColumnsSize, joinBridge, joinProcessorFactory.createHashBuildAndProbeJoinProcessor(), duplicateSetMap, partitionFunction);
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            throw new UnsupportedOperationException("Parallel hash build cannot be duplicated");
        }
    }
}
