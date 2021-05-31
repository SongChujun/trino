package io.trino.operator;

import com.google.common.util.concurrent.ListenableFuture;
import io.trino.spi.Page;
import io.trino.sql.planner.plan.PlanNodeId;

import java.util.List;
import java.util.Stack;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;

public class OuterJoinResultProcessingOperator
        implements Operator
{

    private final OperatorContext operatorContext;
    private final PlanNodeId planNodeId;
    private HashBuildAndProbeTableBundle tableBundle;
    private final AdaptiveJoinBridge joinBridge;
    private final Stack<Page> pageBuffer;
    private final PartitionFunction buildPartitionFunction;
    private final PartitionFunction probePartitionFunction;

    public static class OuterJoinResultProcessingOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final AdaptiveJoinBridge joinBridge;
        private final PartitionFunction buildPartitionFunction;
        private final PartitionFunction probePartitionFunction;
        private boolean closed;

        public OuterJoinResultProcessingOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                AdaptiveJoinBridge joinBridge,
                PartitionFunction buildPartitionFunction,
                PartitionFunction probePartitionFunction

        )
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.joinBridge = requireNonNull(joinBridge, "lookupSourceFactoryManager is null");
            this.buildPartitionFunction = buildPartitionFunction;
            this.probePartitionFunction = probePartitionFunction;
        }

        @Override
        public OuterJoinResultProcessingOperator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, OuterJoinResultProcessingOperator.class.getSimpleName());

//            verify(partitionIndex < lookupSourceFactory.partitions());
            return new OuterJoinResultProcessingOperator(operatorContext,joinBridge,buildPartitionFunction,probePartitionFunction);
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

    public OuterJoinResultProcessingOperator(
            OperatorContext operatorContext,
            PlanNodeId planNodeId,
            AdaptiveJoinBridge joinBridge,
            PartitionFunction buildPartitionFunction,
            PartitionFunction probePartitionFunction
    )
    {
        this.operatorContext = operatorContext;
        this.planNodeId = planNodeId;
        this.joinBridge = joinBridge;
        this.tableBundle = null;
        this.buildPartitionFunction = buildPartitionFunction;
        this.probePartitionFunction = probePartitionFunction;
        this.pageBuffer = new Stack<>();
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
        Page[] extractedPages = extractPages(page);
        if (this.tableBundle==null) {
            int partition = buildPartitionFunction.getPartition(extractedPages[0],0); //0 is hard code here, in theory, all the positions have the same partition
            this.tableBundle = joinBridge.getTableBundle(partition);
        }
        pageBuffer.add(tableBundle.processBuildSidePage(extractedPages[0]));
        pageBuffer.add(tableBundle.processProbeSidePage(extractedPages[1]));
    }

    //specification: left: buildSide, middle: probeSide, right: middle
    private Page[] extractPages(Page page) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Page getOutput()
    {
        if (pageBuffer.isEmpty())
        {
            return null;
        } else {
            return pageBuffer.pop();
        }
    }

    @Override
    public boolean isFinished()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void finish() {
        return;
    }

}
