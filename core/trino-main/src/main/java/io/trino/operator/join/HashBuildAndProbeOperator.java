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

public class HashBuildAndProbeOperator implements Operator
{
    public static class HashBuildAndProbeOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private boolean closed;
        private AdaptiveJoinBridge joinBridge;
        private final PartitionFunction partitionFunction;

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
            return new HashBuildAndProbeOperator(
                    operatorContext,
                    joinBridge,
                    partitionFunction);
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
    private static final double INDEX_COMPACTION_ON_REVOCATION_TARGET = 0.8;

    private final OperatorContext operatorContext;
    private final LocalMemoryContext localUserMemoryContext;
    private final LocalMemoryContext localRevocableMemoryContext;
    private final HashCollisionsCounter hashCollisionsCounter;


    private final AdaptiveJoinBridge joinBridge;
    private HashBuildAndProbeTable table;
    private final PartitionFunction partitionFunction;


    public HashBuildAndProbeOperator(
            OperatorContext operatorContext,
            AdaptiveJoinBridge joinBridge,
            PartitionFunction partitionFunction
    )
    {
        this.operatorContext = operatorContext;
        this.localUserMemoryContext = operatorContext.localUserMemoryContext();
        this.localRevocableMemoryContext = operatorContext.localRevocableMemoryContext();
        this.hashCollisionsCounter = new HashCollisionsCounter(operatorContext);
        operatorContext.setInfoSupplier(hashCollisionsCounter);
        this.joinBridge = joinBridge;
        this.partitionFunction = partitionFunction;
        this.table = null;
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean needsInput()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addInput(Page page)
    {
        if (this.table!=null) {
            int partition = partitionFunction.getPartition(page,0); //0 is hard code here, in theory, all the positions have the same partition
            this.table = joinBridge.getHashTable(partition);
        }
        table.addPage(page);
    }

    @Override
    public ListenableFuture<?> startMemoryRevoke() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void finishMemoryRevoke() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Page getOutput()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void finish() {
        throw new UnsupportedOperationException();
    }

    @Override public boolean isFinished() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
        throw new UnsupportedOperationException();
    }

}
