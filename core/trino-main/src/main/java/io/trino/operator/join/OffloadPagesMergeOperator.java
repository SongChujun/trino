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
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.log.Logger;
import io.trino.operator.DriverContext;
import io.trino.operator.Operator;
import io.trino.operator.OperatorContext;
import io.trino.operator.OperatorFactory;
import io.trino.operator.PagesIndex;
import io.trino.operator.PagesIndexComparator;
import io.trino.spi.Page;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.type.Type;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.type.BlockTypeOperators;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.connector.SortOrder.ASC_NULLS_LAST;
import static java.util.Objects.requireNonNull;

public class OffloadPagesMergeOperator
        implements Operator
{
    public static class OffloadPagesMergeOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final List<Type> leftTypes;
        private final List<Type> rightTypes;
        private final List<Integer> leftMergeChannels;
        private final List<Integer> rightMergeChannels;
        private final List<Integer> leftOutputChannels;
        private final List<Integer> rightOutputChannels;
        List<BlockTypeOperators.BlockPositionComparison> joinEqualOperators;
        private final SortMergeJoinBridge bridge;
        private boolean closed;
        private final SortOperator.SortOperatorFactory.Mode mode;

        public OffloadPagesMergeOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                List<Type> leftTypes,
                List<Type> rightTypes,
                List<Integer> leftMergeChannels,
                List<Integer> rightMergeChannels,
                List<Integer> leftOutputChannels,
                List<Integer> rightOutputChannels,
                List<BlockTypeOperators.BlockPositionComparison> joinEqualOperators,
                SortMergeJoinBridge bridge,
                SortOperator.SortOperatorFactory.Mode mode)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.leftTypes = requireNonNull(leftTypes);
            this.rightTypes = requireNonNull(rightTypes);
            this.leftMergeChannels = requireNonNull(leftMergeChannels);
            this.rightMergeChannels = requireNonNull(rightMergeChannels);
            this.leftOutputChannels = requireNonNull(leftOutputChannels);
            this.rightOutputChannels = requireNonNull(rightOutputChannels);
            this.joinEqualOperators = requireNonNull(joinEqualOperators);
            this.bridge = requireNonNull(bridge);
            this.mode = requireNonNull(mode);
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, OffloadPagesMergeOperator.class.getSimpleName());
            ImmutableList.Builder<SortOrder> sortOrder = ImmutableList.builder();

            for (int i = 0; i < leftMergeChannels.size(); i++) {
                sortOrder.add(ASC_NULLS_LAST);
            }

            List<PagesIndex> upPagesIndexPair = bridge.getNextUpSortedPagesPair();
            List<PagesIndex> downPagesIndexPair = bridge.getNextDownSortedPagesPair();

            PagesIndex leftUpPagesIndex = upPagesIndexPair.get(0);
            PagesIndex leftDownPagesIndex = downPagesIndexPair.get(0);
            PagesIndex rightUpPagesIndex = upPagesIndexPair.get(1);
            PagesIndex rightDownPagesIndex = downPagesIndexPair.get(1);
            PagesIndexComparator leftPagesIndexComparator = leftUpPagesIndex.createPagesIndexComparator(leftMergeChannels, sortOrder.build()).getComparator();
            PagesIndexComparator rightPagesIndexComparator = rightUpPagesIndex.createPagesIndexComparator(rightMergeChannels, sortOrder.build()).getComparator();

            List<Type> leftOutputTypes = leftOutputChannels.stream().map(leftTypes::get).collect(toImmutableList());
            List<Type> rightOutputTypes = rightOutputChannels.stream().map(rightTypes::get).collect(toImmutableList());

            SortMergePageBuilder pageBuilder = new SortMergePageBuilder(leftUpPagesIndex, rightUpPagesIndex, leftOutputTypes, rightOutputTypes, leftOutputChannels, rightOutputChannels);
            return new OffloadPagesMergeOperator(operatorContext, leftMergeChannels, rightMergeChannels, leftUpPagesIndex, leftDownPagesIndex, rightUpPagesIndex, rightDownPagesIndex,
                    leftPagesIndexComparator, rightPagesIndexComparator, bridge.getNextFinishedFuture(), joinEqualOperators,
                    pageBuilder, mode);
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            throw new UnsupportedOperationException("Source operator factories cannot be duplicated");
        }
    }

    private final PagesIndex leftUpSortedPagesIndex;
    private final PagesIndex leftDownSortedPagesIndex;
    private final PagesIndex rightUpSortedPagesIndex;
    private final PagesIndex rightDownSortedPagesIndex;
    private final PagesMergeOperator pagesMergeOperator;
    private final SortOperator.SortOperatorFactory.Mode mode;
    private boolean resultMerged;

    private final ExecutorService executor;

    private static final Logger log = Logger.get(OffloadPagesMergeOperator.class);

    public OffloadPagesMergeOperator(OperatorContext operatorContext,
            List<Integer> leftMergeChannels,
            List<Integer> rightMergeChannels,
            PagesIndex leftUpSortedPagesIndex,
            PagesIndex leftDownSortedPagesIndex,
            PagesIndex rightUpSortedPagesIndex,
            PagesIndex rightDownSortedPagesIndex,
            PagesIndexComparator leftPagesIndexComparator,
            PagesIndexComparator rightPagesIndexComparator,
            SettableFuture<Boolean> sortFinishedFuture,
            List<BlockTypeOperators.BlockPositionComparison> joinEqualOperators,
            SortMergePageBuilder pageBuilder,
            SortOperator.SortOperatorFactory.Mode mode)
    {
        this.leftUpSortedPagesIndex = leftUpSortedPagesIndex;
        this.leftDownSortedPagesIndex = leftDownSortedPagesIndex;
        this.rightUpSortedPagesIndex = rightUpSortedPagesIndex;
        this.rightDownSortedPagesIndex = rightDownSortedPagesIndex;
        pagesMergeOperator = new PagesMergeOperator(operatorContext, leftMergeChannels, rightMergeChannels, leftUpSortedPagesIndex, rightUpSortedPagesIndex,
                leftPagesIndexComparator, rightPagesIndexComparator, sortFinishedFuture, joinEqualOperators, pageBuilder);
        this.mode = mode;
        this.resultMerged = false;
        this.executor = Executors.newFixedThreadPool(2);
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return pagesMergeOperator.getOperatorContext();
    }

    @Override
    public void finish()
    {
        pagesMergeOperator.finish();
    }

    @Override
    public boolean isFinished()
    {
        return pagesMergeOperator.isFinished();
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        return pagesMergeOperator.isBlocked();
    }

    @Override
    public boolean needsInput()
    {
        return false;
    }

    @Override
    public void addInput(Page page)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Page getOutput()
    {
        if ((leftDownSortedPagesIndex.getPositionCount() > 0) || (rightDownSortedPagesIndex.getPositionCount() > 0) || ((mode == SortOperator.SortOperatorFactory.Mode.DYNAMIC) && !resultMerged)) {
            log.debug("start adding merge to task queue");
            if (leftDownSortedPagesIndex.getPositionCount()>0)
            {
                leftUpSortedPagesIndex.mergePagesIndex(leftDownSortedPagesIndex, mode);
                leftDownSortedPagesIndex.clear();
            }
            if (rightDownSortedPagesIndex.getPositionCount()>0)
            {
                rightUpSortedPagesIndex.mergePagesIndex(rightDownSortedPagesIndex, mode);
                rightDownSortedPagesIndex.clear();
            }

            resultMerged = true;
        }
        return pagesMergeOperator.getOutput();
    }

    private static class MergeTask
            implements Callable<Integer>
    {
        PagesIndex leftPagesIndex;

        PagesIndex rightPagesIndex;

        SortOperator.SortOperatorFactory.Mode mode;

        public MergeTask(PagesIndex leftPagesIndex, PagesIndex rightPagesIndex, SortOperator.SortOperatorFactory.Mode mode)
        {
            this.leftPagesIndex = leftPagesIndex;
            this.rightPagesIndex = rightPagesIndex;
            this.mode = mode;
        }

        @Override
        public Integer call()
        {
            leftPagesIndex.mergePagesIndex(rightPagesIndex, mode);
            return 1;
        }
    }

    @Override
    public void close()
    {
        pagesMergeOperator.close();
    }
}
