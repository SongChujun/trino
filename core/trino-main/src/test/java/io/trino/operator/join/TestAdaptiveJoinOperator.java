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
import com.google.common.collect.Iterables;
import com.google.common.primitives.Ints;
import io.airlift.units.DataSize;
import io.trino.RowPagesBuilder;
import io.trino.execution.NodeTaskMap;
import io.trino.execution.scheduler.NodeScheduler;
import io.trino.execution.scheduler.NodeSchedulerConfig;
import io.trino.execution.scheduler.UniformNodeSelectorFactory;
import io.trino.metadata.InMemoryNodeManager;
import io.trino.operator.Driver;
import io.trino.operator.DriverContext;
import io.trino.operator.HashGenerator;
import io.trino.operator.InterpretedHashGenerator;
import io.trino.operator.Operator;
import io.trino.operator.OperatorFactories;
import io.trino.operator.OperatorFactory;
import io.trino.operator.PartitionFunction;
import io.trino.operator.PipelineContext;
import io.trino.operator.PrecomputedHashGenerator;
import io.trino.operator.TaskContext;
import io.trino.operator.TrinoOperatorFactories;
import io.trino.operator.ValuesOperator;
import io.trino.operator.exchange.LocalExchange;
import io.trino.operator.exchange.LocalExchangeSinkOperator;
import io.trino.operator.exchange.LocalExchangeSourceOperator;
import io.trino.operator.exchange.LocalPartitionGenerator;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.planner.NodePartitioningManager;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.testing.MaterializedResult;
import io.trino.testing.TestingTaskContext;
import io.trino.type.BlockTypeOperators;
import io.trino.util.FinalizerService;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.Function;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.RowPagesBuilder.rowPagesBuilder;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.operator.PipelineExecutionStrategy.UNGROUPED_EXECUTION;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.FIXED_PASSTHROUGH_DISTRIBUTION;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;

@Test(singleThreaded = true)
public class TestAdaptiveJoinOperator
{
    private static final int PARTITION_COUNT = 4;
    private static final BlockTypeOperators TYPE_OPERATOR_FACTORY = new BlockTypeOperators(new TypeOperators());

    private final OperatorFactories operatorFactories;

    private ExecutorService executor;
    private ScheduledExecutorService scheduledExecutor;
    private NodePartitioningManager nodePartitioningManager;

    public TestAdaptiveJoinOperator()
    {
        this(new TrinoOperatorFactories());
    }

    protected TestAdaptiveJoinOperator(OperatorFactories operatorFactories)
    {
        this.operatorFactories = requireNonNull(operatorFactories, "operatorFactories is null");
    }

    @DataProvider(name = "hashJoinTestValues")
    public static Object[][] hashJoinTestValuesProvider()
    {
        return new Object[][] {
                {true, true, true},
                {true, true, false},
                {true, false, true},
                {true, false, false},
                {false, true, true},
                {false, true, false},
                {false, false, true},
                {false, false, false}};
    }

    private static <T> List<T> concat(List<T> initialElements, List<T> moreElements)
    {
        return ImmutableList.copyOf(Iterables.concat(initialElements, moreElements));
    }

    private static List<Integer> rangeList(int endExclusive)
    {
        return IntStream.range(0, endExclusive)
                .boxed()
                .collect(toImmutableList());
    }

    private static void runDriverInThread(ExecutorService executor, Driver driver)
    {
        executor.execute(() -> {
            if (!driver.isFinished()) {
                try {
                    driver.process();
                }
                catch (TrinoException e) {
                    driver.getDriverContext().failed(e);
                    return;
                }
                runDriverInThread(executor, driver);
            }
        });
    }

    @BeforeMethod
    public void setUp()
    {
        // Before/AfterMethod is chosen here because the executor needs to be shutdown
        // after every single test case to terminate outstanding threads, if any.

        // The line below is the same as newCachedThreadPool(daemonThreadsNamed(...)) except RejectionExecutionHandler.
        // RejectionExecutionHandler is set to DiscardPolicy (instead of the default AbortPolicy) here.
        // Otherwise, a large number of RejectedExecutionException will flood logging, resulting in Travis failure.
        executor = new ThreadPoolExecutor(
                0,
                Integer.MAX_VALUE,
                60L,
                SECONDS,
                new SynchronousQueue<>(),
                daemonThreadsNamed("test-executor-%s"),
                new ThreadPoolExecutor.DiscardPolicy());
        scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed(getClass().getSimpleName() + "-scheduledExecutor-%s"));

        NodeScheduler nodeScheduler = new NodeScheduler(new UniformNodeSelectorFactory(
                new InMemoryNodeManager(),
                new NodeSchedulerConfig().setIncludeCoordinator(true),
                new NodeTaskMap(new FinalizerService())));
        nodePartitioningManager = new NodePartitioningManager(nodeScheduler, new BlockTypeOperators(new TypeOperators()));
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown()
    {
        executor.shutdownNow();
        scheduledExecutor.shutdownNow();
    }

    @Test(dataProvider = "hashJoinTestValues")
    public void testTableBasic(boolean parallelBuild, boolean probeHashEnabled, boolean buildHashEnabled)
    {
        TaskContext taskContext = createTaskContext();

        int length = 100;
        List<Type> types = ImmutableList.of(BIGINT, BIGINT, BIGINT);
        OptionalInt hashChannel = OptionalInt.of(types.size() - 1);
        List<Integer> joinChannels = Ints.asList(0);
        List<Type> buildOutputTypes = types.subList(0, 2);
        int expectedPositions = length;
        Optional<List<Integer>> buildOutputChannels = Optional.of(Ints.asList(0, 1));
        JoinProbe.JoinProbeFactory buildJoinProbeFactory = new JoinProbe.JoinProbeFactory(buildOutputChannels.get().stream().mapToInt(i -> i).toArray(), joinChannels, hashChannel);

        int partitionCount = parallelBuild ? PARTITION_COUNT : 1;

        // build factory
        RowPagesBuilder buildPages = rowPagesBuilder(buildHashEnabled, Ints.asList(0), ImmutableList.of(BIGINT, BIGINT))
                .addSequencePage(10, 20, 30)
                .addSequencePage(10, 30, 40);

        List<Page> buildInput = buildPages.build();

        AdaptiveJoinBridge joinBridge = new AdaptiveJoinBridge(
                types, hashChannel, joinChannels, buildOutputTypes, buildOutputChannels, TYPE_OPERATOR_FACTORY, length, LookupJoinOperatorFactory.JoinType.INNER,
                true, false, partitionCount);

        PartitionFunction buildPartitionFunction = getLocalPartitionGenerator(hashChannel, joinChannels, types, partitionCount);

        HashBuildAndProbeOperator.HashBuildAndProbeOperatorFactory hashBuildOperatorFactory = new HashBuildAndProbeOperator.HashBuildAndProbeOperatorFactory(
                0, new PlanNodeId("0"), buildPartitionFunction, joinBridge);

        BuildSideSetup buildSideSetup = setupBuildSide(parallelBuild, taskContext, joinChannels, buildPages, hashBuildOperatorFactory);

        List<Symbol> leftSymbols = ImmutableList.<Symbol>builder()
                .add(new Symbol("L1"))
                .add(new Symbol("L2"))
                .build();

        List<Symbol> rightSymbols = ImmutableList.<Symbol>builder()
                .add(new Symbol("R1"))
                .add(new Symbol("R2"))
                .build();

        List<Symbol> leftJoinSymbols = ImmutableList.<Symbol>builder()
                .add(new Symbol("L1"))
                .build();

        List<Symbol> rightJoinSymbols = ImmutableList.<Symbol>builder()
                .add(new Symbol("R1"))
                .build();

        List<Symbol> outputSymbols = ImmutableList.<Symbol>builder()
                .addAll(leftSymbols).addAll(rightSymbols).build();

        OperatorFactory outerJoinProcessingOperatorFactory = new OuterJoinResultProcessingOperator.OuterJoinResultProcessingOperatorFactory(
                0, new PlanNodeId("0"), joinBridge, true, buildPartitionFunction, leftSymbols, rightSymbols,
                leftJoinSymbols, rightJoinSymbols, outputSymbols);

        // probe factory
        RowPagesBuilder probePages = rowPagesBuilder(probeHashEnabled, Ints.asList(0), ImmutableList.of(BIGINT, BIGINT))
                .addSequencePage(10, 25, 1000)
                .addSequencePage(3, 35, 1000);

        List<Page> probeInput = probePages.build();

        BuildSideSetup outerSideSetup = setupBuildSide(parallelBuild, taskContext, Ints.asList(2, 3), probePages, outerJoinProcessingOperatorFactory);

        LocalExchange.LocalExchangeFactory localExchangeFactory = new LocalExchange.LocalExchangeFactory(
                nodePartitioningManager,
                taskContext.getSession(),
                FIXED_PASSTHROUGH_DISTRIBUTION, // explicitly use pass through exachange here
                partitionCount,
                types,
                null,
                Optional.empty(),
                UNGROUPED_EXECUTION,
                DataSize.of(32, DataSize.Unit.MEGABYTE),
                TYPE_OPERATOR_FACTORY);

        instantiateBuildDrivers(buildSideSetup, taskContext, localExchangeFactory);
        instantiateBuildDrivers(outerSideSetup, taskContext, localExchangeFactory);

        runDriver(buildSideSetup);
        runDriver(outerSideSetup);

        OperatorFactory sourceOperatorFactory = new LocalExchangeSourceOperator.LocalExchangeSourceOperatorFactory(0, new PlanNodeId("0"), localExchangeFactory);

        instantiateMergeDrivers(taskContext, sourceOperatorFactory);

        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), concat(probePages.getTypesWithoutHash(), buildPages.getTypesWithoutHash()))
                .row(25L, 1000L, 25L, 35L)
                .row(26L, 1001L, 26L, 36L)
                .row(27L, 1002L, 27L, 37L)
                .row(28L, 1003L, 28L, 38L)
                .row(29L, 1004L, 29L, 39L)
                .row(30L, 1005L, 30L, 40L)
                .row(31L, 1006L, 31L, 41L)
                .row(32L, 1007L, 32L, 42L)
                .row(33L, 1008L, 33L, 43L)
                .row(34L, 1009L, 34L, 44L)
                .row(35L, 1000L, 35L, 45L)
                .row(36L, 1001L, 36L, 46L)
                .row(37L, 1002L, 37L, 47L)
                .build();

//        List<Page> actual = new ArrayList<>();
//        for (Page page: buildInput) {
//            table.addPage(page);
//        }
//        for (Page page:probeInput) {
//            actual.add(table.joinPage(page));
//        }
//        assertPagesEqualIgnoreOrder(taskContext.getSession(),actual,expected,false,Optional.empty());
//
//        table.reset();
    }

    private LocalPartitionGenerator getLocalPartitionGenerator(OptionalInt hashChannel, List<Integer> joinChannels, List<Type> types, int partitionCount)
    {
        HashGenerator hashGenerator = null;
        requireNonNull(hashChannel, "probeHashChannel is null");
        if (hashChannel.isPresent()) {
            hashGenerator = new PrecomputedHashGenerator(hashChannel.getAsInt());
        }
        else {
            requireNonNull(joinChannels, "probeJoinChannels is null");
            List<Type> hashTypes = joinChannels.stream()
                    .map(types::get)
                    .collect(toImmutableList());
            hashGenerator = new InterpretedHashGenerator(hashTypes, joinChannels, TYPE_OPERATOR_FACTORY);
        }
        return new LocalPartitionGenerator(hashGenerator, partitionCount);
    }

    private TaskContext createTaskContext()
    {
        return TestingTaskContext.createTaskContext(executor, scheduledExecutor, TEST_SESSION);
    }

    private TestAdaptiveJoinOperator.BuildSideSetup setupBuildSide(
            boolean parallelBuild,
            TaskContext taskContext,
            List<Integer> hashChannels,
            RowPagesBuilder buildPages,
            OperatorFactory buildOperatorFactory)
    {
        int partitionCount = parallelBuild ? PARTITION_COUNT : 1;
        LocalExchange.LocalExchangeFactory localExchangeFactory = new LocalExchange.LocalExchangeFactory(
                nodePartitioningManager,
                taskContext.getSession(),
                FIXED_HASH_DISTRIBUTION,
                partitionCount,
                buildPages.getTypes(),
                hashChannels,
                buildPages.getHashChannel(),
                UNGROUPED_EXECUTION,
                DataSize.of(32, DataSize.Unit.MEGABYTE),
                TYPE_OPERATOR_FACTORY);
        LocalExchange.LocalExchangeSinkFactoryId localExchangeSinkFactoryId = localExchangeFactory.newSinkFactoryId();
        localExchangeFactory.noMoreSinkFactories();

        // collect input data into the partitioned exchange
        DriverContext collectDriverContext = taskContext.addPipelineContext(0, true, true, false).addDriverContext();
        ValuesOperator.ValuesOperatorFactory valuesOperatorFactory = new ValuesOperator.ValuesOperatorFactory(0, new PlanNodeId("values"), buildPages.build());
        LocalExchangeSinkOperator.LocalExchangeSinkOperatorFactory sinkOperatorFactory = new LocalExchangeSinkOperator.LocalExchangeSinkOperatorFactory(localExchangeFactory, 1, new PlanNodeId("sink"), localExchangeSinkFactoryId, Function.identity());
        Driver sourceDriver = Driver.createDriver(collectDriverContext,
                valuesOperatorFactory.createOperator(collectDriverContext),
                sinkOperatorFactory.createOperator(collectDriverContext));
        valuesOperatorFactory.noMoreOperators();
        sinkOperatorFactory.noMoreOperators();

        while (!sourceDriver.isFinished()) {
            sourceDriver.process();
        }
        LocalExchangeSourceOperator.LocalExchangeSourceOperatorFactory sourceOperatorFactory = new LocalExchangeSourceOperator.LocalExchangeSourceOperatorFactory(0, new PlanNodeId("source"), localExchangeFactory);
        return new TestAdaptiveJoinOperator.BuildSideSetup(buildOperatorFactory, sourceOperatorFactory, partitionCount);
    }

    private void instantiateBuildDrivers(BuildSideSetup buildSideSetup, TaskContext taskContext, LocalExchange.LocalExchangeFactory localExchangeFactory)
    {
        OperatorFactory sinkOperatorFactory = new LocalExchangeSinkOperator.LocalExchangeSinkOperatorFactory(
                localExchangeFactory,
                0,
                new PlanNodeId("0"),
                localExchangeFactory.newSinkFactoryId(),
                Function.identity());

        PipelineContext buildPipeline = taskContext.addPipelineContext(1, true, true, false);
        List<Driver> buildDrivers = new ArrayList<>();
        List<Operator> buildOperators = new ArrayList<>();
        for (int i = 0; i < buildSideSetup.getPartitionCount(); i++) {
            DriverContext buildDriverContext = buildPipeline.addDriverContext();
            Operator buildOperator = buildSideSetup.getBuildOperatorFactory().createOperator(buildDriverContext);
            Operator sinkOperator = sinkOperatorFactory.createOperator(buildDriverContext);
            Driver driver = Driver.createDriver(
                    buildDriverContext,
                    buildSideSetup.getBuildSideSourceOperatorFactory().createOperator(buildDriverContext),
                    buildOperator, sinkOperator);
            buildDrivers.add(driver);
            buildOperators.add(buildOperator);
        }

        buildSideSetup.setDriversAndOperators(buildDrivers, buildOperators);
    }

    private void instantiateMergeDrivers(TaskContext taskContext, OperatorFactory sourceOperatorFactory)
    {
        PipelineContext buildPipeline = taskContext.addPipelineContext(1, true, true, false);
        DriverContext buildDriverContext = buildPipeline.addDriverContext();
        Operator sourceOperator = sourceOperatorFactory.createOperator(buildDriverContext);
        Driver driver = Driver.createDriver(
                buildDriverContext,
                sourceOperator);

        runDriverInThread(executor, driver);
    }

    private void runDriver(BuildSideSetup buildSideSetup)
    {
        requireNonNull(buildSideSetup, "buildSideSetup is null");
        List<Driver> buildDrivers = buildSideSetup.getBuildDrivers();
        for (Driver buildDriver : buildDrivers) {
            runDriverInThread(executor, buildDriver);
        }
    }

    private static class BuildSideSetup
    {
        private final OperatorFactory buildOperatorFactory;
        private final LocalExchangeSourceOperator.LocalExchangeSourceOperatorFactory buildSideSourceOperatorFactory;
        private final int partitionCount;
        private List<Driver> buildDrivers;
        private List<Operator> buildOperators;

        BuildSideSetup(OperatorFactory buildOperatorFactory, LocalExchangeSourceOperator.LocalExchangeSourceOperatorFactory buildSideSourceOperatorFactory, int partitionCount)
        {
            this.buildOperatorFactory = requireNonNull(buildOperatorFactory, "buildOperatorFactory is null");
            this.buildSideSourceOperatorFactory = buildSideSourceOperatorFactory;
            this.partitionCount = partitionCount;
        }

        void setDriversAndOperators(List<Driver> buildDrivers, List<Operator> buildOperators)
        {
            checkArgument(buildDrivers.size() == buildOperators.size());
            this.buildDrivers = ImmutableList.copyOf(buildDrivers);
            this.buildOperators = ImmutableList.copyOf(buildOperators);
        }

        OperatorFactory getBuildOperatorFactory()
        {
            return buildOperatorFactory;
        }

        public LocalExchangeSourceOperator.LocalExchangeSourceOperatorFactory getBuildSideSourceOperatorFactory()
        {
            return buildSideSourceOperatorFactory;
        }

        public int getPartitionCount()
        {
            return partitionCount;
        }

        List<Driver> getBuildDrivers()
        {
            checkState(buildDrivers != null, "buildDrivers is not initialized yet");
            return buildDrivers;
        }

        List<Operator> getBuildOperators()
        {
            checkState(buildOperators != null, "buildDrivers is not initialized yet");
            return buildOperators;
        }
    }
}
