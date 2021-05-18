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
//        private final JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactoryManager;
        private final List<Integer> outputChannels;
        private final List<Integer> hashChannels;
        private final OptionalInt preComputedHashChannel;
        private final Optional<JoinFilterFunctionCompiler.JoinFilterFunctionFactory> filterFunctionFactory;
        private final Optional<Integer> sortChannel;
        private final List<JoinFilterFunctionCompiler.JoinFilterFunctionFactory> searchFunctionFactories;
        private final PagesIndex.Factory pagesIndexFactory;

        private final int expectedPositions;
        private final boolean spillEnabled;
        private final SingleStreamSpillerFactory singleStreamSpillerFactory;

        private final Map<Lifespan, Integer> partitionIndexManager = new HashMap<>();
        private boolean closed;

        public HashBuildAndProbeOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                List<Integer> outputChannels,
                List<Integer> hashChannels,
                OptionalInt preComputedHashChannel,
                Optional<JoinFilterFunctionCompiler.JoinFilterFunctionFactory> filterFunctionFactory,
                Optional<Integer> sortChannel,
                List<JoinFilterFunctionCompiler.JoinFilterFunctionFactory> searchFunctionFactories,
                int expectedPositions,
                PagesIndex.Factory pagesIndexFactory,
                boolean spillEnabled,
                SingleStreamSpillerFactory singleStreamSpillerFactory)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            requireNonNull(sortChannel, "sortChannel cannot be null");
            requireNonNull(searchFunctionFactories, "searchFunctionFactories is null");
            checkArgument(sortChannel.isPresent() != searchFunctionFactories.isEmpty(), "both or none sortChannel and searchFunctionFactories must be set");

            this.outputChannels = ImmutableList.copyOf(requireNonNull(outputChannels, "outputChannels is null"));
            this.hashChannels = ImmutableList.copyOf(requireNonNull(hashChannels, "hashChannels is null"));
            this.preComputedHashChannel = requireNonNull(preComputedHashChannel, "preComputedHashChannel is null");
            this.filterFunctionFactory = requireNonNull(filterFunctionFactory, "filterFunctionFactory is null");
            this.sortChannel = sortChannel;
            this.searchFunctionFactories = ImmutableList.copyOf(searchFunctionFactories);
            this.pagesIndexFactory = requireNonNull(pagesIndexFactory, "pagesIndexFactory is null");
            this.spillEnabled = spillEnabled;
            this.singleStreamSpillerFactory = requireNonNull(singleStreamSpillerFactory, "singleStreamSpillerFactory is null");

            this.expectedPositions = expectedPositions;
        }

        @Override
        public HashBuilderOperator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, HashBuildAndProbeOperator.class.getSimpleName());

//            PartitionedLookupSourceFactory lookupSourceFactory = this.lookupSourceFactoryManager.getJoinBridge(driverContext.getLifespan());
            int partitionIndex = getAndIncrementPartitionIndex(driverContext.getLifespan());
            verify(partitionIndex < lookupSourceFactory.partitions());
            return new HashBuildAndProbeOperator(
                    operatorContext,
                    lookupSourceFactory,
                    partitionIndex,
                    outputChannels,
                    hashChannels,
                    preComputedHashChannel,
                    filterFunctionFactory,
                    sortChannel,
                    searchFunctionFactories,
                    expectedPositions,
                    pagesIndexFactory,
                    spillEnabled,
                    singleStreamSpillerFactory);
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

        private int getAndIncrementPartitionIndex(Lifespan lifespan)
        {
            return partitionIndexManager.compute(lifespan, (k, v) -> v == null ? 1 : v + 1) - 1;
        }
    }

    private static final double INDEX_COMPACTION_ON_REVOCATION_TARGET = 0.8;

    private final OperatorContext operatorContext;
    private final LocalMemoryContext localUserMemoryContext;
    private final LocalMemoryContext localRevocableMemoryContext;
    private final PartitionedLookupSourceFactory lookupSourceFactory;
    private final ListenableFuture<?> lookupSourceFactoryDestroyed;
    private final int partitionIndex;

    private final List<Integer> outputChannels;
    private final List<Integer> hashChannels;
    private final OptionalInt preComputedHashChannel;
    private final Optional<JoinFilterFunctionCompiler.JoinFilterFunctionFactory> filterFunctionFactory;
    private final Optional<Integer> sortChannel;
    private final List<JoinFilterFunctionCompiler.JoinFilterFunctionFactory> searchFunctionFactories;

    private final PagesIndex index;

    private final boolean spillEnabled;
    private final SingleStreamSpillerFactory singleStreamSpillerFactory;

    private final HashCollisionsCounter hashCollisionsCounter;

    private HashBuilderOperator.State state = HashBuilderOperator.State.CONSUMING_INPUT;
    private Optional<ListenableFuture<?>> lookupSourceNotNeeded = Optional.empty();
    private final SpilledLookupSourceHandle spilledLookupSourceHandle = new SpilledLookupSourceHandle();
    private Optional<SingleStreamSpiller> spiller = Optional.empty();
    private ListenableFuture<?> spillInProgress = NOT_BLOCKED;
    private Optional<ListenableFuture<List<Page>>> unspillInProgress = Optional.empty();
    @Nullable
    private LookupSourceSupplier lookupSourceSupplier;
    private OptionalLong lookupSourceChecksum = OptionalLong.empty();

    private Optional<Runnable> finishMemoryRevoke = Optional.empty();
    private HashBuildAndProbeTable hashBuildAndProbeTable;

    public HashBuildAndProbeOperator(
            OperatorContext operatorContext,
            PartitionedLookupSourceFactory lookupSourceFactory,
            int partitionIndex,
            List<Integer> outputChannels,
            List<Integer> hashChannels,
            OptionalInt preComputedHashChannel,
            Optional<JoinFilterFunctionCompiler.JoinFilterFunctionFactory> filterFunctionFactory,
            Optional<Integer> sortChannel,
            List<JoinFilterFunctionCompiler.JoinFilterFunctionFactory> searchFunctionFactories,
            int expectedPositions,
            PagesIndex.Factory pagesIndexFactory,
            boolean spillEnabled,
            SingleStreamSpillerFactory singleStreamSpillerFactory)
    {
        requireNonNull(pagesIndexFactory, "pagesIndexFactory is null");

        this.operatorContext = operatorContext;
        this.partitionIndex = partitionIndex;
        this.filterFunctionFactory = filterFunctionFactory;
        this.sortChannel = sortChannel;
        this.searchFunctionFactories = searchFunctionFactories;
        this.localUserMemoryContext = operatorContext.localUserMemoryContext();
        this.localRevocableMemoryContext = operatorContext.localRevocableMemoryContext();

        this.index = pagesIndexFactory.newPagesIndex(lookupSourceFactory.getTypes(), expectedPositions);
        this.lookupSourceFactory = lookupSourceFactory;
        lookupSourceFactoryDestroyed = lookupSourceFactory.isDestroyed();

        this.outputChannels = outputChannels;
        this.hashChannels = hashChannels;
        this.preComputedHashChannel = preComputedHashChannel;

        this.hashCollisionsCounter = new HashCollisionsCounter(operatorContext);
        operatorContext.setInfoSupplier(hashCollisionsCounter);

        this.spillEnabled = spillEnabled;
        this.singleStreamSpillerFactory = requireNonNull(singleStreamSpillerFactory, "singleStreamSpillerFactory is null");
        this.hashBuildAndProbeTable = hashBuildAndProbeTable;

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
//        throw new IllegalStateException("Unhandled state: " + state);
    }

    @Override
    public boolean needsInput()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addInput(Page page)
    {
        hashBuildAndProbeTable.addPage(page);
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
//        return  hashBuildAndProbeTable.;
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
