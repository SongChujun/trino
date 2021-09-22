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
import io.trino.spi.block.IntArrayBlock;
import io.trino.spi.block.LongArrayBlock;
import io.trino.sql.planner.plan.PlanNodeId;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Stack;
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
    private final Stack<Page> pageBuffer;
    private final List<Integer> leftPrimaryKeyChannels;
    private final List<Integer> leftJoinChannels;
    private final List<Integer> rightJoinChannels;
    private final List<Integer> outputChannels;
    private final Integer leftColumnsSize;
    private final ListenableFuture<Boolean> hashBuildFinishedFuture;
    private final HashBuildAndProbeTable hashTable;
    private boolean isFinished;
    private Set<String> duplicateSet;

    public OuterJoinResultProcessingOperator(
            OperatorContext operatorContext,
            boolean isInnerJoin,
            List<Integer> leftPrimaryKeyChannels,
            List<Integer> leftJoinChannels,
            List<Integer> rightJoinChannels,
            List<Integer> outputChannels,
            Integer leftColumnsSize,
            HashBuildAndProbeTable table)
    {
        this.operatorContext = operatorContext;
        this.isInnerJoin = isInnerJoin;
        this.hashTable = requireNonNull(table);
        this.leftPrimaryKeyChannels = requireNonNull(leftPrimaryKeyChannels);
        this.leftJoinChannels = requireNonNull(leftJoinChannels);
        this.rightJoinChannels = requireNonNull(rightJoinChannels);
        this.outputChannels = requireNonNull(outputChannels);
        this.leftColumnsSize = requireNonNull(leftColumnsSize);
        this.pageBuffer = new Stack<>();
        this.hashBuildFinishedFuture = table.getBuildFinishedFuture();
        this.duplicateSet = new HashSet<>(10000);
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
        Page[] extractedPages = extractPages(page, isInnerJoin);
        Page joinResult = hashTable.joinPage(extractedPages[0]);
        if (joinResult != null) {
            pageBuffer.add(joinResult);
        }
        pageBuffer.add(extractedPages[1]);
    }

    //specification: left: probeSide, right: outerSide
    //input format: probeSideData, outerSideData
    private Page[] extractPages(Page page, boolean isInnerJoin)
    {
        //nullIndicators encoding: 0 stands for null, t, 1 stands for s, null, 2 stands for s,t ,3 for null,null
        int[] nullIndicators = new int[page.getPositionCount()];
        boolean[] duplicateIndicators = new boolean[page.getPositionCount()];
        for (int i = 0; i < page.getPositionCount(); i++) {
            int finalI = i;
            boolean leftNull = leftJoinChannels.stream().anyMatch(ch -> page.getBlock(ch).isNull(finalI));
            boolean rightNull = rightJoinChannels.stream().anyMatch(ch -> page.getBlock(ch).isNull(finalI));
            String primaryKeyStr = tupleToString(page, leftPrimaryKeyChannels, i);
            if (duplicateSet.contains(primaryKeyStr)) {
                duplicateIndicators[i] = true;
            }
            else {
                duplicateSet.add(primaryKeyStr);
                duplicateIndicators[i] = false;
            }
            if ((leftNull) && (!rightNull)) {
                nullIndicators[i] = 0;
            }
            else if ((!leftNull) && (rightNull)) {
                nullIndicators[i] = 1;
            }
            else if ((!leftNull) && (!rightNull)) {
                nullIndicators[i] = 2;
            }
            else {
                nullIndicators[i] = 3;
            }
        }
        int[] leftRetainedPositions = IntStream.range(0, nullIndicators.length).filter(idx -> (nullIndicators[idx] == 1 || nullIndicators[idx] == 2) && !duplicateIndicators[idx]).toArray();
        int[] outputRetainedPositions = IntStream.range(0, nullIndicators.length).filter(idx -> (nullIndicators[idx] == 2)).toArray();
        Page leftPage = page.copyPositions(leftRetainedPositions, 0, leftRetainedPositions.length).getColumns(Stream.concat(IntStream.range(0, leftColumnsSize).boxed(), Stream.of(page.getChannelCount() - 1)).mapToInt(i -> i).toArray());
        Page outputPage = page.copyPositions(outputRetainedPositions, 0, outputRetainedPositions.length).getColumns(outputChannels.stream().mapToInt(i -> i).toArray());
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
        if (pageBuffer.isEmpty()) {
            return null;
        }
        else {
            return pageBuffer.pop();
        }
    }

    @Override
    public boolean isFinished()
    {
        return isFinished && pageBuffer.isEmpty();
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
                Integer leftColumnsSize)
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
        }

        @Override
        public OuterJoinResultProcessingOperator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, OuterJoinResultProcessingOperator.class.getSimpleName());
            Integer localPartitioningIndex = driverContext.getLocalPartitioningIndex();
            return new OuterJoinResultProcessingOperator(operatorContext, isInnerJoin,
                    leftPrimaryKeyChannels, leftJoinChannels, rightJoinChannels, outputChannels, leftColumnsSize, joinBridge.getHashTable(localPartitioningIndex));
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
