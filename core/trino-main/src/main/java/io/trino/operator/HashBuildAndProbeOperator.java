package io.trino.operator;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import io.trino.execution.Lifespan;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.spi.Page;
import io.trino.spiller.SingleStreamSpiller;
import io.trino.spiller.SingleStreamSpillerFactory;
import io.trino.sql.gen.JoinFilterFunctionCompiler;
import io.trino.sql.planner.plan.PlanNodeId;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;

public class HashBuildAndProbeOperator implements Operator
{
    public static class HashBuildAndProbeOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final boolean isBuildSide;
        private boolean closed;
        private AdaptiveJoinBridge joinBridge;
        private final PartitionFunction partitionFunction;

        public HashBuildAndProbeOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                boolean isBuildSide,
                PartitionFunction partitionFunction,
                AdaptiveJoinBridge joinBridge)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.isBuildSide = isBuildSide;
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
                    isBuildSide,
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
    private final HashBuildAndProbeTableBundle tableBundle;
    private final boolean isBuildSide;
    private final PartitionFunction partitionFunction;
    private Page result;


    public HashBuildAndProbeOperator(
            OperatorContext operatorContext,
            AdaptiveJoinBridge joinBridge,
            boolean isBuildSide,
            PartitionFunction partitionFunction
            )
    {
        this.operatorContext = operatorContext;
        this.localUserMemoryContext = operatorContext.localUserMemoryContext();
        this.localRevocableMemoryContext = operatorContext.localRevocableMemoryContext();
        this.hashCollisionsCounter = new HashCollisionsCounter(operatorContext);
        operatorContext.setInfoSupplier(hashCollisionsCounter);
        this.joinBridge = joinBridge;
        this.isBuildSide = isBuildSide;
        this.partitionFunction = partitionFunction;
        this.tableBundle = null;
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
        if (this.tableBundle!=null) {
            int partition = partitionFunction.getPartition(page,0); //0 is hard code here, in theory, all the positions have the same partition
            this.tableBundle = joinBridge.getTableBundle(partition);
        }
        if (isBuildSide) {
            result = tableBundle.processBuildSidePage(page);
        } else {
            result = tableBundle.processProbeSidePage(page);
        }
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
        Page res = result;
        result = null;
        return res;
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
