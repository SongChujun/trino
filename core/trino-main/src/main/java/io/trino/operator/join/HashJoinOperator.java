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
import io.trino.sql.planner.plan.PlanNodeId;

import java.util.List;
import java.util.Stack;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class HashJoinOperator
        implements Operator
{
    private final OperatorContext operatorContext;
    private final Stack<Page> outputPageBuffer;
    private AdaptiveJoinBridge joinBridge;
    private final HashBuildAndProbeJoinProcessor joinProcessor;
    private boolean isFinished;

    public HashJoinOperator(
            OperatorContext operatorContext,
            AdaptiveJoinBridge joinBridge,
            HashBuildAndProbeJoinProcessor joinProcessor)
    {
        this.operatorContext = operatorContext;
        this.joinBridge = requireNonNull(joinBridge);
        this.joinProcessor = requireNonNull(joinProcessor);
        this.outputPageBuffer = new Stack<>();
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
        List<Page> joinResult = joinProcessor.join(page);
        if (joinResult != null) {
            outputPageBuffer.addAll(joinResult);
        }
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

    public static class HashJoinOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private AdaptiveJoinBridge joinBridge;
        private final HashBuildAndProbeJoinProcessor.HashBuildAndProbeJoinProcessorFactory joinProcessorFactory;
        private boolean closed;

        public HashJoinOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                AdaptiveJoinBridge joinBridge,
                HashBuildAndProbeJoinProcessor.HashBuildAndProbeJoinProcessorFactory joinProcessorFactory)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.joinBridge = joinBridge;
            this.joinProcessorFactory = requireNonNull(joinProcessorFactory, "lookupSourceFactoryManager is null");
        }

        @Override
        public HashJoinOperator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, HashJoinOperator.class.getSimpleName());
//            Integer localPartitioningIndex = driverContext.getLocalPartitioningIndex();
            return new HashJoinOperator(operatorContext, joinBridge, joinProcessorFactory.createHashBuildAndProbeJoinProcessor());
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
