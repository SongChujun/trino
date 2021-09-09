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
import io.trino.memory.context.LocalMemoryContext;
import io.trino.operator.DriverContext;
import io.trino.operator.HashCollisionsCounter;
import io.trino.operator.Operator;
import io.trino.operator.OperatorContext;
import io.trino.operator.OperatorFactory;
import io.trino.operator.PartitionFunction;
import io.trino.spi.Page;
import io.trino.sql.planner.plan.PlanNodeId;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class HashBuildAndProbeOperator
        implements Operator
{
    private static final double INDEX_COMPACTION_ON_REVOCATION_TARGET = 0.8;
    private final OperatorContext operatorContext;
    private final LocalMemoryContext localUserMemoryContext;
    private final LocalMemoryContext localRevocableMemoryContext;
    private final HashCollisionsCounter hashCollisionsCounter;
    private final PartitionFunction partitionFunction;
    private final HashBuildAndProbeTable table;
    private boolean isFinished;
    private final int partitioningIndex;

    public HashBuildAndProbeOperator(
            OperatorContext operatorContext,
            PartitionFunction partitionFunction,
            HashBuildAndProbeTable table,
            int partitioningIndex)
    {
        this.operatorContext = operatorContext;
        this.localUserMemoryContext = operatorContext.localUserMemoryContext();
        this.localRevocableMemoryContext = operatorContext.localRevocableMemoryContext();
        this.hashCollisionsCounter = new HashCollisionsCounter(operatorContext);
        operatorContext.setInfoSupplier(hashCollisionsCounter);
        this.partitionFunction = partitionFunction;
        this.table = table;
        this.partitioningIndex = partitioningIndex;
        isFinished = false;
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        return NOT_BLOCKED;
    }

    @Override
    public boolean needsInput()
    {
        return true;
    }

    @Override
    public void addInput(Page page)
    {
        table.addPage(page);
    }

    @Override
    public ListenableFuture<?> startMemoryRevoke()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void finishMemoryRevoke()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Page getOutput()
    {
        return null;
    }

    @Override
    public void finish()
    {
        table.setBuildFinished();
        isFinished = true;
    }

    @Override
    public boolean isFinished()
    {
        return isFinished;
    }

//    @Override
//    public void close()
//    {
////        return;//subject to change
//    }

    public static class HashBuildAndProbeOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final AdaptiveJoinBridge joinBridge;
        private final PartitionFunction partitionFunction;
        private boolean closed;

        public HashBuildAndProbeOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                PartitionFunction partitionFunction,
                AdaptiveJoinBridge joinBridge)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.partitionFunction = partitionFunction;
            this.joinBridge = joinBridge;
        }

        @Override
        public HashBuildAndProbeOperator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, HashBuildAndProbeOperator.class.getSimpleName());
            Integer index = driverContext.getLocalPartitioningIndex();
            return new HashBuildAndProbeOperator(
                    operatorContext,
                    partitionFunction, joinBridge.getHashTable(index), index);
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
