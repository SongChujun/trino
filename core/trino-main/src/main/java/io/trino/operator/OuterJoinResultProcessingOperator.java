package io.trino.operator;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import io.trino.execution.Lifespan;
import io.trino.spi.Page;
import io.trino.spiller.SingleStreamSpillerFactory;
import io.trino.sql.gen.JoinFilterFunctionCompiler;
import io.trino.sql.planner.plan.PlanNodeId;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;

public class OuterJoinResultProcessingOperator
        implements Operator
{

    private final OperatorContext operatorContext;
    private Page current;
    private IncrementalJoinBridge joinBridge;

    public static class OuterJoinResultProcessingOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final IncrementalJoinBridge joinBridge;
        private boolean closed;

        public OuterJoinResultProcessingOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                IncrementalJoinBridge joinBridge
        )
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.joinBridge = requireNonNull(joinBridge, "lookupSourceFactoryManager is null");

        }

        @Override
        public OuterJoinResultProcessingOperator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, OuterJoinResultProcessingOperator.class.getSimpleName());

//            verify(partitionIndex < lookupSourceFactory.partitions());
            return new OuterJoinResultProcessingOperator();
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
            IncrementalJoinBridge joinBridge
    )
    {
        this.operatorContext = operatorContext;
        this.joinBridge = joinBridge;
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
        current = page;
        List<Page> extractedPages = extractPages(page);
        joinBridge.buildSideInsert(extractedPages.get(0));
        joinBridge.probeSideProbe(extractedPages.get(0));
        joinBridge.probeSideInsert(extractedPages.get(2));
        joinBridge.buildSideProbe(extractedPages.get(2));

    }

    //specification: left: buildSide, middle: buildAndProbeSide, right: probeSide
    private List<Page> extractPages(Page page) {
        return null;
    }

    @Override
    public Page getOutput()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isFinished()
    {
        throw new UnsupportedOperationException();
    }









}
