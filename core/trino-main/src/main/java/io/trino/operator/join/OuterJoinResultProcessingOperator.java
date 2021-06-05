package io.trino.operator.join;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import io.trino.operator.DriverContext;
import io.trino.operator.Operator;
import io.trino.operator.OperatorContext;
import io.trino.operator.OperatorFactory;
import io.trino.operator.PartitionFunction;
import io.trino.spi.Page;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.plan.PlanNodeId;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class OuterJoinResultProcessingOperator
        implements Operator
{
    public static class OuterJoinResultProcessingOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final AdaptiveJoinBridge joinBridge;
        private final boolean isInnerJoin;
        private final PartitionFunction buildPartitionFunction;
        private boolean closed;
        private final List<Symbol> leftSymbols;
        private final List<Symbol> rightSymbols;
        private final List<Symbol> leftJoinSymbols;
        private final List<Symbol> rightJoinSymbols;
        private final List<Symbol> outputSymbols;

        public OuterJoinResultProcessingOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                AdaptiveJoinBridge joinBridge,
                boolean isInnerJoin,
                PartitionFunction buildPartitionFunction,
                List<Symbol> leftSymbols,
                List<Symbol> rightSymbols,
                List<Symbol> leftJoinSymbols,
                List<Symbol> rightJoinSymbols,
                List<Symbol> outputSymbols
        )
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.joinBridge = requireNonNull(joinBridge, "lookupSourceFactoryManager is null");
            this.isInnerJoin = isInnerJoin;
            this.buildPartitionFunction = buildPartitionFunction;
            this.leftSymbols = leftSymbols;
            this.rightSymbols = rightSymbols;
            this.leftJoinSymbols = leftJoinSymbols;
            this.rightJoinSymbols = rightJoinSymbols;
            this.outputSymbols = outputSymbols;
        }

        @Override
        public OuterJoinResultProcessingOperator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, OuterJoinResultProcessingOperator.class.getSimpleName());

            return new OuterJoinResultProcessingOperator(operatorContext,planNodeId,isInnerJoin,joinBridge,buildPartitionFunction,
                    leftSymbols,rightSymbols,leftJoinSymbols,rightJoinSymbols,outputSymbols);
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

    private final OperatorContext operatorContext;
    private final PlanNodeId planNodeId;
    private final boolean isInnerJoin;
    private HashBuildAndProbeTable hashTable;
    private final AdaptiveJoinBridge joinBridge;
    private final Stack<Page> pageBuffer;
    private final PartitionFunction buildPartitionFunction;
    private final List<Symbol> leftSymbols;
    private final List<Symbol> rightSymbols;
    private final List<Symbol> leftJoinSymbols;
    private final List<Symbol> rightJoinSymbols;
    private final List<Symbol> outputSymbols;

    public OuterJoinResultProcessingOperator(
            OperatorContext operatorContext,
            PlanNodeId planNodeId,
            boolean isInnerJoin,
            AdaptiveJoinBridge joinBridge,
            PartitionFunction buildPartitionFunction,
            List<Symbol> leftSymbols,
            List<Symbol> rightSymbols,
            List<Symbol> leftJoinSymbols,
            List<Symbol> rightJoinSymbols,
            List<Symbol> outputSymbols
    )
    {
        this.operatorContext = operatorContext;
        this.planNodeId = planNodeId;
        this.isInnerJoin = isInnerJoin;
        this.joinBridge = joinBridge;
        this.hashTable = null;
        this.buildPartitionFunction = buildPartitionFunction;
        this.leftSymbols = leftSymbols;
        this.rightSymbols = rightSymbols;
        this.leftJoinSymbols = leftJoinSymbols;
        this.rightJoinSymbols = rightJoinSymbols;
        this.outputSymbols = outputSymbols;
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
        Page[] extractedPages = extractPages(page,isInnerJoin);
        if (this.hashTable==null) {
            int partition = buildPartitionFunction.getPartition(extractedPages[0],0); //0 is hard code here, in theory, all the positions have the same partition
            this.hashTable = joinBridge.getHashTable(partition);
        }
        Page joinResult = hashTable.joinPage(extractedPages[0]);
        if (joinResult!=null) {
            pageBuffer.add(joinResult);
        }
        pageBuffer.add(extractedPages[1]);
    }

    //specification: left: probeSide, right: outerSide
    //input format: probeSideData, outerSideData
    private Page[] extractPages(Page page, boolean isInnerJoin) {
        assert isInnerJoin;
        //nullIndicators encoding: 0 stands for null,t, 1 stands for t,null, 2 stands for s,t ,3 for null,null
        int[] nullIndicators = new int[page.getPositionCount()];
        Map<Symbol, Integer> layout = makeLayout(leftSymbols,rightSymbols);
        for (int i = 0;i<page.getPositionCount();i++) {
            boolean leftNull = false;
            boolean rightNull = false;
            for (int channel: leftJoinSymbols.stream().map(layout::get).collect(Collectors.toList())) {
                if (page.getBlock(channel).isNull(i)) {
                    leftNull = true;
                    break;
                }
            }
            for (int channel: rightJoinSymbols.stream().map(layout::get).collect(Collectors.toList())) {
                if (page.getBlock(channel).isNull(i)) {
                    rightNull = true;
                    break;
                }
            }
            if ((leftNull)&&(!rightNull)) {
                nullIndicators[i] = 0;
            } else if ((!leftNull)&&(rightNull)) {
                nullIndicators[i] = 1;
            } else if ((!leftNull)&&(!rightNull)) {
                nullIndicators[i] = 2;
            } else {
                nullIndicators[i] = 3;
            }
        }
        int[] leftRetainedPositions = Arrays.stream(nullIndicators).filter(value -> (value==1||value==2)).toArray();
        int[] outputRetainedPositions = Arrays.stream(nullIndicators).filter(value -> value==2).toArray();
        Page leftPage = page.copyPositions(leftRetainedPositions,0,leftRetainedPositions.length).getColumns(IntStream.range(0, leftSymbols.size()).toArray());
        Page outputPage = page.copyPositions(outputRetainedPositions,0,outputRetainedPositions.length).getColumns(outputSymbols.stream().map(layout::get).mapToInt(i->i).toArray());
        return new Page[]{leftPage,outputPage};
    }

    private ImmutableMap<Symbol, Integer> makeLayout(List<Symbol> leftSymbols, List<Symbol> rightSymbols)
    {
        ImmutableMap.Builder<Symbol, Integer> outputMappings = ImmutableMap.builder();
        int channel = 0;
        for (Symbol symbol : leftSymbols) {
            outputMappings.put(symbol, channel);
            channel++;
        }
        for (Symbol symbol : rightSymbols) {
            outputMappings.put(symbol, channel);
            channel++;
        }

        return outputMappings.build();
    }

    private List<Integer> rangeList(int endExclusive)
    {
        return IntStream.range(0, endExclusive)
                .boxed()
                .collect(toImmutableList());
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
