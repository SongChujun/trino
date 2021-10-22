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
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.type.BlockTypeOperators;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Stack;

import static com.google.common.base.Preconditions.checkState;
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
    private final ListenableFuture<Boolean> hashBuildFinishedFuture;
    private final HashBuildAndProbeTable hashTable;
    private boolean isFinished;
    private Set<String> duplicateSet;
    private long allExtractTime;
    private long duplicateDetectionTime;
    List<BlockTypeOperators.BlockPositionEqual> equalOperators;
    private Page currentPrimaryKeyPage;
    private int currentPrimaryKeyRow;

    public OuterJoinResultProcessingOperator(
            OperatorContext operatorContext,
            boolean isInnerJoin,
            List<Integer> leftPrimaryKeyChannels,
            List<Integer> leftJoinChannels,
            List<Integer> rightJoinChannels,
            List<Integer> outputChannels,
            Integer leftColumnsSize,
            HashBuildAndProbeTable table,
            List<BlockTypeOperators.BlockPositionEqual> equalOperators)
    {
        this.operatorContext = operatorContext;
        this.isInnerJoin = isInnerJoin;
        this.hashTable = requireNonNull(table);
        this.leftPrimaryKeyChannels = requireNonNull(leftPrimaryKeyChannels);
        this.leftJoinChannels = requireNonNull(leftJoinChannels);
        this.rightJoinChannels = requireNonNull(rightJoinChannels);
        this.outputChannels = requireNonNull(outputChannels);
        this.leftColumnsSize = requireNonNull(leftColumnsSize);
        this.outputPageBuffer = new Stack<>();
        this.hashBuildFinishedFuture = table.getBuildFinishedFuture();
        this.duplicateSet = new HashSet<>(150000);
        this.allExtractTime = 0;
        this.duplicateDetectionTime = 0;
        this.equalOperators = equalOperators;
        this.currentPrimaryKeyPage = null;
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        return hashBuildFinishedFuture;
    }

    @Override
    public boolean needsInput()
    {
        return hashBuildFinishedFuture.isDone();
    }

    @Override
    public void addInput(Page page)
    {
        long processStartTime = System.currentTimeMillis();
        Page[] extractedPages = extractPages(page, isInnerJoin);
        duplicateDetectionTime += System.currentTimeMillis() - processStartTime;
//        this.outputEntryCnt +=extractedPages[1].getPositionCount();
        List<Page> joinResult = hashTable.joinPage(extractedPages[0]);
        allExtractTime += System.currentTimeMillis() - processStartTime;
        if (joinResult != null) {
//            System.out.println(extractedPages[0].getPositionCount() - joinResult.getPositionCount());
            outputPageBuffer.addAll(joinResult);
//            joinResult.forEach(outputPageBuffer::add);
//            outputPageBuffer.add(joinResult);
        }
        outputPageBuffer.add(extractedPages[1]);
    }

    //specification: left: probeSide, right: outerSide
    //input format: probeSideData, outerSideData
    private Page[] extractPages(Page page, boolean isInnerJoin)
    {
        int[] leftRetainedPositions = new int[page.getPositionCount()];
        int[] outputRetainedPositions = new int[page.getPositionCount()];
        int leftRetainedPositionsPtr = 0;
        int outputRetainedPositionsPtr = 0;
        for (int i = 0; i < page.getPositionCount(); i++) {
            boolean rightNull = page.getBlock(rightJoinChannels.get(0)).isNull(i);
            if (!primaryKeyColumnsEqual(page, i)) {
                leftRetainedPositions[leftRetainedPositionsPtr] = i;
                leftRetainedPositionsPtr += 1;
            }
            if (rightNull) {
//                leftRetainedPositions[leftRetainedPositionsPtr] = i;
//                leftRetainedPositionsPtr += 1;
            }
            else {
                outputRetainedPositions[outputRetainedPositionsPtr] = i;
                outputRetainedPositionsPtr += 1;
            }
        }

        int[] leftColumnIdxs = new int[leftColumnsSize + 1];
        for (int i = 0; i < leftColumnsSize; i++) {
            leftColumnIdxs[i] = i;
        }

        int[] outputColumnIdxs = new int[outputChannels.size()];
        for (int i = 0; i < outputChannels.size(); i++) {
            outputColumnIdxs[i] = outputChannels.get(i);
        }
        leftColumnIdxs[leftColumnsSize] = page.getChannelCount() - 1;
        Page leftPage = page.copyPositions(leftRetainedPositions, 0, leftRetainedPositionsPtr).getColumns(leftColumnIdxs);
        Page outputPage = page.copyPositions(outputRetainedPositions, 0, outputRetainedPositionsPtr).getColumns(outputColumnIdxs);
        return new Page[] {leftPage, outputPage};
    }

    private Boolean primaryKeyColumnsEqual(Page page, int row)
    {
        if (currentPrimaryKeyPage == null) {
            currentPrimaryKeyPage = page;
            currentPrimaryKeyRow = row;
            return false;
        }
        for (int i = 0; i < leftPrimaryKeyChannels.size(); i++) {
            BlockTypeOperators.BlockPositionEqual equalOperator = equalOperators.get(i);
            Block comingBlock = page.getBlock(leftPrimaryKeyChannels.get(i));
            Block currentBlock = currentPrimaryKeyPage.getBlock(leftPrimaryKeyChannels.get(i));
            if (!equalOperator.equal(currentBlock, currentPrimaryKeyRow, comingBlock, row)) {
                currentPrimaryKeyPage = page;
                currentPrimaryKeyRow = row;
                return false;
            }
        }
        return true;
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
        private final AdaptiveJoinBridge joinBridge;
        private final boolean isInnerJoin;
        private final List<Integer> leftPrimaryKeyChannels;
        private final List<Integer> leftJoinChannels;
        private final List<Integer> rightJoinChannels;
        private final List<Integer> outputChannels;
        private final Integer leftColumnsSize;
        private final List<BlockTypeOperators.BlockPositionEqual> equalOperators;
        private boolean closed;

        public OuterJoinResultProcessingOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                AdaptiveJoinBridge joinBridge,
                boolean isInnerJoin,
                List<Integer> leftPrimaryKeyChannels,
                List<Integer> leftJoinChannels,
                List<Integer> rightJoinChannels,
                List<Integer> outputChannels,
                Integer leftColumnsSize,
                List<BlockTypeOperators.BlockPositionEqual> equalOperators)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.joinBridge = requireNonNull(joinBridge, "lookupSourceFactoryManager is null");
            this.isInnerJoin = isInnerJoin;
            this.leftPrimaryKeyChannels = requireNonNull(leftPrimaryKeyChannels);
            this.leftJoinChannels = requireNonNull(leftJoinChannels);
            this.rightJoinChannels = requireNonNull(rightJoinChannels);
            this.outputChannels = requireNonNull(outputChannels);
            this.leftColumnsSize = requireNonNull(leftColumnsSize);
            this.equalOperators = requireNonNull(equalOperators);
        }

        @Override
        public OuterJoinResultProcessingOperator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, OuterJoinResultProcessingOperator.class.getSimpleName());
            Integer localPartitioningIndex = driverContext.getLocalPartitioningIndex();
            return new OuterJoinResultProcessingOperator(operatorContext, isInnerJoin,
                    leftPrimaryKeyChannels, leftJoinChannels, rightJoinChannels, outputChannels, leftColumnsSize, joinBridge.getHashTable(localPartitioningIndex), equalOperators);
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
