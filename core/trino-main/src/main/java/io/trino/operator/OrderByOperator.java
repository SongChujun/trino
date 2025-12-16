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
package io.trino.operator;

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.ListenableFuture;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.type.Type;
import io.trino.spiller.Spiller;
import io.trino.spiller.SpillerFactory;
import io.trino.sql.gen.OrderingCompiler;
import io.trino.sql.planner.plan.PlanNodeId;

import java.lang.invoke.MethodHandle;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.base.Verify.verifyNotNull;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterators.transform;
import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static io.airlift.concurrent.MoreFutures.asVoid;
import static io.airlift.concurrent.MoreFutures.checkSuccess;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.trino.util.MergeSortedPages.mergeSortedPages;
import static java.util.Objects.requireNonNull;

public class OrderByOperator
        implements Operator
{
    public static class OrderByOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final List<Type> sourceTypes;
        private final List<Integer> outputChannels;
        private final int expectedPositions;
        private final List<Integer> sortChannels;
        private final List<SortOrder> sortOrder;
        private final PagesIndex.Factory pagesIndexFactory;
        private final boolean spillEnabled;
        private final Optional<SpillerFactory> spillerFactory;
        private final OrderingCompiler orderingCompiler;
        private final FlatHashStrategyCompiler hashStrategyCompiler;
        private final int[] layoutToInput; // mapping from layout index -> input index
        private final boolean rowBasedOrderBy;

        private boolean closed;

        public OrderByOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                List<? extends Type> sourceTypes,
                List<Integer> outputChannels,
                int expectedPositions,
                List<Integer> sortChannels,
                List<SortOrder> sortOrder,
                PagesIndex.Factory pagesIndexFactory,
                boolean spillEnabled,
                Optional<SpillerFactory> spillerFactory,
                OrderingCompiler orderingCompiler,
                FlatHashStrategyCompiler hashStrategyCompiler,
                int[] layoutToInput,
                boolean rowBasedOrderBy)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.sourceTypes = ImmutableList.copyOf(requireNonNull(sourceTypes, "sourceTypes is null"));
            this.outputChannels = requireNonNull(outputChannels, "outputChannels is null");
            this.expectedPositions = expectedPositions;
            this.sortChannels = ImmutableList.copyOf(requireNonNull(sortChannels, "sortChannels is null"));
            this.sortOrder = ImmutableList.copyOf(requireNonNull(sortOrder, "sortOrder is null"));

            this.pagesIndexFactory = requireNonNull(pagesIndexFactory, "pagesIndexFactory is null");
            this.spillEnabled = spillEnabled;
            this.spillerFactory = requireNonNull(spillerFactory, "spillerFactory is null");
            this.orderingCompiler = requireNonNull(orderingCompiler, "orderingCompiler is null");
            this.hashStrategyCompiler = requireNonNull(hashStrategyCompiler, "hashStrategyCompiler is null");
            this.layoutToInput = requireNonNull(layoutToInput, "layoutToInput is null");
            this.rowBasedOrderBy = rowBasedOrderBy;
            checkArgument(!spillEnabled || spillerFactory.isPresent(), "Spiller Factory is not present when spill is enabled");
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");

            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, OrderByOperator.class.getSimpleName());
            return new OrderByOperator(
                    operatorContext,
                    sourceTypes,
                    outputChannels,
                    expectedPositions,
                    sortChannels,
                    sortOrder,
                    pagesIndexFactory,
                    spillEnabled,
                    spillerFactory,
                    orderingCompiler,
                    hashStrategyCompiler,
                    layoutToInput,
                    rowBasedOrderBy);
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new OrderByOperatorFactory(
                    operatorId,
                    planNodeId,
                    sourceTypes,
                    outputChannels,
                    expectedPositions,
                    sortChannels,
                    sortOrder,
                    pagesIndexFactory,
                    spillEnabled,
                    spillerFactory,
                    orderingCompiler,
                    hashStrategyCompiler,
                    layoutToInput,
                    rowBasedOrderBy);
        }
    }

    private enum State
    {
        NEEDS_INPUT,
        HAS_OUTPUT,
        FINISHED
    }

    private final int small = 7;
    private final int medium = 40;

    private final OperatorContext operatorContext;
    private final List<Integer> sortChannels;
    private final List<SortOrder> sortOrder;
    private final int[] outputChannels;
    private final LocalMemoryContext revocableMemoryContext;
    private final LocalMemoryContext localUserMemoryContext;

    private final boolean useRowBased;
    // Row-based sorting using RowContainer
    private RowContainer rowContainer;
    private RowContainerOrdering rowOrdering;
    // Columnar sorting with PagesIndex (existing representation)
    private PagesIndex pageIndex;
    private PagesIndexOrdering pagesIndexOrdering;

    private final List<Type> sourceTypes;

    private final boolean spillEnabled;
    private final Optional<SpillerFactory> spillerFactory;
    private final OrderingCompiler orderingCompiler;
    private final FlatHashStrategyCompiler hashStrategyCompiler;
    private final int[] layoutToInput;

    private Optional<Spiller> spiller = Optional.empty();
    private ListenableFuture<Void> spillInProgress = immediateVoidFuture();
    private Optional<Runnable> finishMemoryRevoke = Optional.empty();

    private Iterator<Optional<Page>> sortedPages;

    private State state = State.NEEDS_INPUT;

    public OrderByOperator(
            OperatorContext operatorContext,
            List<Type> sourceTypes,
            List<Integer> outputChannels,
            int expectedPositions,
            List<Integer> sortChannels,
            List<SortOrder> sortOrder,
            PagesIndex.Factory pagesIndexFactory,
            boolean spillEnabled,
            Optional<SpillerFactory> spillerFactory,
            OrderingCompiler orderingCompiler,
            FlatHashStrategyCompiler hashStrategyCompiler,
            int[] layoutToInput,
            boolean useRowBased)
    {
        requireNonNull(pagesIndexFactory, "pagesIndexFactory is null");

        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.outputChannels = Ints.toArray(requireNonNull(outputChannels, "outputChannels is null"));
        this.sortChannels = ImmutableList.copyOf(requireNonNull(sortChannels, "sortChannels is null"));
        this.sortOrder = ImmutableList.copyOf(requireNonNull(sortOrder, "sortOrder is null"));
        this.sourceTypes = ImmutableList.copyOf(requireNonNull(sourceTypes, "sourceTypes is null"));
        this.localUserMemoryContext = operatorContext.localUserMemoryContext();
        this.revocableMemoryContext = operatorContext.localRevocableMemoryContext();

        this.useRowBased = useRowBased;
        this.hashStrategyCompiler = requireNonNull(hashStrategyCompiler, "hashStrategyCompiler is null");
        this.layoutToInput = layoutToInput;
        if (useRowBased) {
            // Build layout types from mapping and create strategy
            List<Type> layoutTypes = java.util.stream.IntStream.range(0, sourceTypes.size())
                    .mapToObj(i -> sourceTypes.get(this.layoutToInput[i]))
                    .collect(toImmutableList());
            FlatHashStrategy flatStrategy = this.hashStrategyCompiler.getFlatHashStrategy(layoutTypes);

            this.rowContainer = new RowContainer(layoutTypes, flatStrategy);

            // Map input sort channels to layout indices
            int[] inputToLayout = new int[this.layoutToInput.length];
            for (int i = 0; i < this.layoutToInput.length; i++) {
                inputToLayout[this.layoutToInput[i]] = i;
            }
            List<Integer> layoutSortChannels = this.sortChannels.stream()
                    .map(ch -> inputToLayout[ch])
                    .collect(toImmutableList());
            // Build a keys-only strategy in sort key order (ascending, NULLS FIRST)
            java.util.List<Type> keyTypes = layoutSortChannels.stream().map(layoutTypes::get).collect(toImmutableList());
            FlatHashStrategy keyStrategy = this.hashStrategyCompiler.getFlatHashStrategy(keyTypes);
            this.rowOrdering = new RowContainerOrdering(this.rowContainer, layoutSortChannels, this.sortOrder, keyStrategy, /*channelsAreLayout*/ true);

            // Translate output channels (specified in input coordinates) to layout coordinates
            for (int i = 0; i < this.outputChannels.length; i++) {
                this.outputChannels[i] = inputToLayout[this.outputChannels[i]];
            }
        }
        else {
            this.pageIndex = pagesIndexFactory.newPagesIndex(sourceTypes, expectedPositions);
            this.pagesIndexOrdering = pageIndex.createPagesIndexComparator(this.sortChannels, this.sortOrder);
        }
        this.spillEnabled = spillEnabled;
        this.spillerFactory = requireNonNull(spillerFactory, "spillerFactory is null");
        this.orderingCompiler = requireNonNull(orderingCompiler, "orderingCompiler is null");
        // fields already set above
        checkArgument(!spillEnabled || spillerFactory.isPresent(), "Spiller Factory is not present when spill is enabled");
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public void finish()
    {
        if (!spillInProgress.isDone()) {
            return;
        }
        checkSuccess(spillInProgress, "spilling failed");
        if (finishMemoryRevoke.isPresent()) {
            return;
        }

        if (state == State.NEEDS_INPUT) {
            state = State.HAS_OUTPUT;

            // Convert revocable memory to user memory as sortedPages holds on to memory so we no longer can revoke.
            if (revocableMemoryContext.getBytes() > 0) {
                long currentRevocableBytes = revocableMemoryContext.getBytes();
                revocableMemoryContext.setBytes(0);
                if (!localUserMemoryContext.trySetBytes(localUserMemoryContext.getBytes() + currentRevocableBytes)) {
                    // TODO: this might fail (even though we have just released memory), but we don't
                    // have a proper way to atomically convert memory reservations
                    revocableMemoryContext.setBytes(currentRevocableBytes);
                    // spill since revocable memory could not be converted to user memory immediately
                    // TODO: this should be asynchronous
                    getFutureValue(spillToDisk());
                    finishMemoryRevoke.orElseThrow().run();
                    finishMemoryRevoke = Optional.empty();
                }
            }

            Iterator<Page> sortedPagesIndex = useRowBased ? getSortedPagesFromRowContainer() : getSortedPagesFromPagesIndex();

            List<WorkProcessor<Page>> spilledPages = getSpilledPages();
            if (spilledPages.isEmpty()) {
                sortedPages = transform(sortedPagesIndex, Optional::of);
            }
            else {
                sortedPages = mergeSpilledAndMemoryPages(spilledPages, sortedPagesIndex).yieldingIterator();
            }
        }
    }

    @Override
    public boolean isFinished()
    {
        return state == State.FINISHED;
    }

    @Override
    public boolean needsInput()
    {
        return state == State.NEEDS_INPUT;
    }

    @Override
    public void addInput(Page page)
    {
        checkState(state == State.NEEDS_INPUT, "Operator is already finishing");
        requireNonNull(page, "page is null");
        checkSuccess(spillInProgress, "spilling failed");

        // TODO: remove when retained memory accounting for pages does not
        // count shared data structures multiple times
        page.compact();
        if (useRowBased) {
            // Reorder page to layout order before appending
            Block[] blocks = new Block[layoutToInput.length];
            for (int i = 0; i < blocks.length; i++) {
                blocks[i] = page.getBlock(layoutToInput[i]);
            }
            Page layoutPage = new Page(page.getPositionCount(), blocks);
            rowContainer.append(layoutPage);
        }
        else {
            pageIndex.addPage(page);
        }
        updateMemoryUsage();
    }

    @Override
    public Page getOutput()
    {
        checkSuccess(spillInProgress, "spilling failed");
        if (state != State.HAS_OUTPUT) {
            return null;
        }

        verifyNotNull(sortedPages, "sortedPages is null");
        if (!sortedPages.hasNext()) {
            state = State.FINISHED;
            return null;
        }

        Optional<Page> next = sortedPages.next();
        if (next.isEmpty()) {
            return null;
        }
        Page nextPage = next.get();
        return nextPage.getColumns(outputChannels);
    }

    @Override
    public ListenableFuture<Void> startMemoryRevoke()
    {
        verify(state == State.NEEDS_INPUT || revocableMemoryContext.getBytes() == 0, "Cannot spill in state: %s", state);
        return spillToDisk();
    }

    private ListenableFuture<Void> spillToDisk()
    {
        checkSuccess(spillInProgress, "spilling failed");

        if (revocableMemoryContext.getBytes() == 0) {
            verify((useRowBased ? rowContainer.size() == 0 : pageIndex.getPositionCount() == 0) || state == State.HAS_OUTPUT);
            finishMemoryRevoke = Optional.of(() -> {});
            return immediateVoidFuture();
        }

        // TODO try pageIndex.compact(); before spilling, as in HashBuilderOperator.startMemoryRevoke()

        if (spiller.isEmpty()) {
            spiller = Optional.of(spillerFactory.get().create(
                    sourceTypes,
                    operatorContext.getSpillContext(),
                    operatorContext.newAggregateUserMemoryContext()));
        }

        if (useRowBased) {
            spillInProgress = asVoid(spiller.get().spill(getSortedPagesFromRowContainer()));
        }
        else {
            pageIndex.sort(pagesIndexOrdering);
            spillInProgress = asVoid(spiller.get().spill(pageIndex.getSortedPages()));
        }
        finishMemoryRevoke = Optional.of(() -> {
            // Reset storage to release memory and accept additional input
            if (useRowBased) {
                // Recreate container and ordering with the same layout mapping
                List<Type> layoutTypes = java.util.stream.IntStream.range(0, sourceTypes.size())
                        .mapToObj(i -> sourceTypes.get(layoutToInput[i]))
                        .collect(toImmutableList());
                FlatHashStrategy flat = hashStrategyCompiler.getFlatHashStrategy(layoutTypes);
                rowContainer = new RowContainer(layoutTypes, flat);

                // rebuild ordering
                int[] inputToLayout = new int[layoutToInput.length];
                for (int i = 0; i < layoutToInput.length; i++) {
                    inputToLayout[layoutToInput[i]] = i;
                }
                List<Integer> layoutSortChannels = sortChannels.stream().map(ch -> inputToLayout[ch]).collect(toImmutableList());
                java.util.List<Type> keyTypes = layoutSortChannels.stream().map(layoutTypes::get).collect(toImmutableList());
                FlatHashStrategy keyStrategy = hashStrategyCompiler.getFlatHashStrategy(keyTypes);
                rowOrdering = new RowContainerOrdering(rowContainer, layoutSortChannels, sortOrder, keyStrategy, true);
            }
            else {
                pageIndex.clear();
            }
            updateMemoryUsage();
        });

        return spillInProgress;
    }

    @Override
    public void finishMemoryRevoke()
    {
        finishMemoryRevoke.orElseThrow().run();
        finishMemoryRevoke = Optional.empty();
    }

    private List<WorkProcessor<Page>> getSpilledPages()
    {
        if (spiller.isEmpty()) {
            return ImmutableList.of();
        }

        return spiller.get().getSpills().stream()
                .map(WorkProcessor::fromIterator)
                .collect(toImmutableList());
    }

    private WorkProcessor<Page> mergeSpilledAndMemoryPages(List<WorkProcessor<Page>> spilledPages, Iterator<Page> sortedPagesIndex)
    {
        List<WorkProcessor<Page>> sortedStreams = ImmutableList.<WorkProcessor<Page>>builder()
                .addAll(spilledPages)
                .add(WorkProcessor.fromIterator(sortedPagesIndex))
                .build();

        List<Type> sortTypes = sortChannels.stream()
                .map(sourceTypes::get)
                .collect(toImmutableList());

        return mergeSortedPages(
                sortedStreams,
                orderingCompiler.compilePageWithPositionComparator(sortTypes, sortChannels, sortOrder),
                sourceTypes,
                operatorContext.aggregateUserMemoryContext(),
                operatorContext.getDriverContext().getYieldSignal());
    }

    private void updateMemoryUsage()
    {
        if (spillEnabled && state == State.NEEDS_INPUT) {
            long retained = useRowBased ? rowContainer.getRetainedSizeInBytes() : pageIndex.getEstimatedSize().toBytes();
            if ((useRowBased ? rowContainer.size() == 0 : pageIndex.getPositionCount() == 0)) {
                localUserMemoryContext.setBytes(retained);
                revocableMemoryContext.setBytes(0L);
            }
            else {
                localUserMemoryContext.setBytes(0);
                revocableMemoryContext.setBytes(retained);
            }
        }
        else {
            revocableMemoryContext.setBytes(0);
            long retained = useRowBased ? rowContainer.getRetainedSizeInBytes() : pageIndex.getEstimatedSize().toBytes();
            if (!localUserMemoryContext.trySetBytes(retained)) {
                if (!useRowBased) {
                    pageIndex.compact();
                    localUserMemoryContext.setBytes(pageIndex.getEstimatedSize().toBytes());
                }
                else {
                    localUserMemoryContext.setBytes(retained);
                }
            }
        }
    }

    @Override
    public void close()
    {
        if (useRowBased) {
            rowContainer = null;
        }
        else {
            pageIndex.clear();
        }
        sortedPages = null;
        spiller.ifPresent(Spiller::close);
    }

    private Iterator<Page> getSortedPagesFromPagesIndex()
    {
        pageIndex.sort(pagesIndexOrdering);
        return pageIndex.getSortedPages();
    }

    private Iterator<Page> getSortedPagesFromRowContainer()
    {
        int[] order = rowContainer.getAliveRowIds();
        quickSortRowIds(order, 0, order.length);

        // Build pages lazily via an iterator to keep memory bounded
        final int batch = 1024; // positions per page batch
        return new Iterator<>()
        {
            private int index;

            @Override
            public boolean hasNext()
            {
                return index < order.length;
            }

            @Override
            public Page next()
            {
                int remaining = order.length - index;
                int len = Math.min(batch, remaining);
                int[] ids = new int[len];
                System.arraycopy(order, index, ids, 0, len);
                index += len;
                return rowContainer.toPage(ids);
            }
        };
    }

    private void quickSortRowIds(int[] ids, int from, int to)
    {
        int len = to - from;
        if (len < small) {
            for (int i = from; i < to; i++) {
                for (int j = i; j > from && (rowOrdering.compare(ids[j - 1], ids[j]) > 0); j--) {
                    swapIds(ids, j, j - 1);
                }
            }
            return;
        }

        int m = from + len / 2;
        if (len > small) {
            int l = from;
            int n = to - 1;
            if (len > medium) {
                int s = len / 8;
                l = median3RowIds(ids, l, l + s, l + 2 * s);
                m = median3RowIds(ids, m - s, m, m + s);
                n = median3RowIds(ids, n - 2 * s, n - s, n);
            }
            m = median3RowIds(ids, l, m, n);
        }

        int a = from;
        int b = a;
        int c = to - 1;
        int d = c;
        while (true) {
            int comparison;
            while (b <= c) {
                comparison = rowOrdering.compare(ids[b], ids[m]);
                if (comparison > 0) {
                    break;
                }
                if (comparison == 0) {
                    if (a == m) {
                        m = b;
                    }
                    else if (b == m) {
                        m = a;
                    }
                    swapIds(ids, a++, b);
                }
                b++;
            }
            while (c >= b) {
                comparison = rowOrdering.compare(ids[c], ids[m]);
                if (comparison < 0) {
                    break;
                }
                if (comparison == 0) {
                    if (c == m) {
                        m = d;
                    }
                    else if (d == m) {
                        m = c;
                    }
                    swapIds(ids, c, d--);
                }
                c--;
            }
            if (b > c) {
                break;
            }
            if (b == m) {
                m = d;
            }
            else if (c == m) {
                m = c;
            }
            swapIds(ids, b++, c--);
        }

        int s;
        int n = to;
        s = Math.min(a - from, b - a);
        vectorSwapIds(ids, from, b - s, s);
        s = Math.min(d - c, n - d - 1);
        vectorSwapIds(ids, b, n - s, s);

        s = b - a;
        if (s > 1) {
            quickSortRowIds(ids, from, from + s);
        }
        s = d - c;
        if (s > 1) {
            quickSortRowIds(ids, n - s, n);
        }
    }

    private int median3RowIds(int[] ids, int a, int b, int c)
    {
        int ab = rowOrdering.compare(ids[a], ids[b]);
        int ac = rowOrdering.compare(ids[a], ids[c]);
        int bc = rowOrdering.compare(ids[b], ids[c]);
        return (ab < 0 ? (bc < 0 ? b : ac < 0 ? c : a) : (bc > 0 ? b : ac > 0 ? c : a));
    }

    private static void vectorSwapIds(int[] ids, int from, int l, int s)
    {
        for (int i = 0; i < s; i++, from++, l++) {
            swapIds(ids, from, l);
        }
    }

    private static void swapIds(int[] ids, int i, int j)
    {
        int t = ids[i];
        ids[i] = ids[j];
        ids[j] = t;
    }

    // Row-based comparator operating directly on RowContainer records using flat layout
    private static final class RowContainerOrdering
    {
        private final RowContainer container;
        private final int[] sortChannels;
        private final int[] layoutSortChannels; // already in layout indices if channelsAreLayout == true
        private final SortOrder[] sortOrders;
        private final io.trino.operator.FlatHashStrategy strategy; // keys-only, ascending, NULLS FIRST
        private final boolean invertSingleKey;
        private final int[] fieldIsNullOffsets; // relative to row fixed value offset
        private final int[] fieldFixedOffsets;  // relative to row fixed value offset
        private final io.trino.spi.type.Type[] types;
        private final io.trino.spi.type.TypeOperators typeOperators;
        private final MethodHandle[] readFlatHandles; // per layout index
        private final MethodHandle[] compareHandles;  // per layout index

        RowContainerOrdering(RowContainer container, List<Integer> sortChannels, List<SortOrder> sortOrders, FlatHashStrategy compareStrategy, boolean channelsAreLayout)
        {
            this.container = container;
            this.sortChannels = com.google.common.primitives.Ints.toArray(sortChannels);
            this.sortOrders = sortOrders.toArray(SortOrder[]::new);
            List<Type> layoutSchema = container.getLayoutTypes();
            this.types = layoutSchema.toArray(new Type[0]);
            if (channelsAreLayout) {
                this.layoutSortChannels = this.sortChannels.clone();
            }
            else {
                this.layoutSortChannels = new int[this.sortChannels.length];
                for (int i = 0; i < this.sortChannels.length; i++) {
                    this.layoutSortChannels[i] = container.inputToLayoutIndex(this.sortChannels[i]);
                }
            }

            // Precompute field offsets within the row's fixed region
            this.fieldIsNullOffsets = new int[types.length];
            this.fieldFixedOffsets = new int[types.length];
            int offset = 0;
            for (int i = 0; i < types.length; i++) {
                fieldIsNullOffsets[i] = offset;
                fieldFixedOffsets[i] = offset + 1;
                offset += 1 + types[i].getFlatFixedSize();
            }

            // Strategy compares keys only in ascending NULLS FIRST
            this.strategy = compareStrategy;
            this.invertSingleKey = (this.layoutSortChannels.length == 1) && !this.sortOrders[0].isAscending();

            // Prepare interpreted operators
            this.typeOperators = new io.trino.spi.type.TypeOperators();
            this.readFlatHandles = new MethodHandle[types.length];
            this.compareHandles = new MethodHandle[types.length];
            for (int i = 0; i < types.length; i++) {
                io.trino.spi.type.Type t = types[i];
                this.readFlatHandles[i] = this.typeOperators.getReadValueOperator(
                        t,
                        io.trino.spi.function.InvocationConvention.simpleConvention(
                                io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL,
                                io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.FLAT));
                // Unordered-last comparison matches existing compiled path for ASC_NULLS_LAST/DESC_NULLS_LAST
                this.compareHandles[i] = this.typeOperators.getComparisonUnorderedLastOperator(
                        t,
                        io.trino.spi.function.InvocationConvention.simpleConvention(
                                io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL,
                                io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL,
                                io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL));
            }
        }

        int compare(int leftRowId, int rightRowId)
        {
            // Obtain row pointers (fixed bytes base + variable chunk/offset)
            RowContainer.RowPointer left = container.getRowPointer(leftRowId);
            RowContainer.RowPointer right = container.getRowPointer(rightRowId);

            // Use compiled strategy (keys-only, asc, NULLS FIRST); invert for single-key DESC
            int cmp = strategy.compareKeysWithNulls(
                    left.fixed, left.fixedOffset, left.variable, left.variableOffset,
                    right.fixed, right.fixedOffset, right.variable, right.variableOffset);
            return invertSingleKey ? -cmp : cmp;
        }

        private int computeVariableOffsetForField(byte[] fixed, int fixedBaseOffset, int targetFieldIndex)
        {
            int offset = 0;
            for (int i = 0; i < targetFieldIndex; i++) {
                if (types[i].isFlatVariableWidth()) {
                    offset += types[i].getFlatVariableWidthLength(fixed, fixedBaseOffset + fieldFixedOffsets[i]);
                }
            }
            return offset;
        }
    }
}
