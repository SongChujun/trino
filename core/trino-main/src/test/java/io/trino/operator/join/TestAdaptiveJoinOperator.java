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
import io.trino.RowPagesBuilder;
import io.trino.execution.NodeTaskMap;
import io.trino.execution.scheduler.NodeScheduler;
import io.trino.execution.scheduler.NodeSchedulerConfig;
import io.trino.execution.scheduler.UniformNodeSelectorFactory;
import io.trino.metadata.InMemoryNodeManager;
import io.trino.operator.HashGenerator;
import io.trino.operator.InterpretedHashGenerator;
import io.trino.operator.OperatorFactories;
import io.trino.operator.OperatorFactory;
import io.trino.operator.PartitionFunction;
import io.trino.operator.PrecomputedHashGenerator;
import io.trino.operator.TaskContext;
import io.trino.operator.TrinoOperatorFactories;
import io.trino.operator.exchange.LocalPartitionGenerator;
import io.trino.spi.Page;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.planner.NodePartitioningManager;
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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Iterators.unmodifiableIterator;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.testing.Assertions.assertEqualsIgnoreOrder;
import static io.trino.RowPagesBuilder.rowPagesBuilder;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.operator.OperatorAssertion.assertOperatorEquals;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.assertions.Assert.assertEquals;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

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

    @Test(dataProvider = "hashJoinTestValues")
    public void testInnerJoin(boolean parallelBuild, boolean probeHashEnabled, boolean buildHashEnabled)
    {
        TaskContext taskContext = createTaskContext();

        // build factory
        RowPagesBuilder buildPages = rowPagesBuilder(buildHashEnabled, Ints.asList(0), ImmutableList.of(VARCHAR, BIGINT, BIGINT))
                .addSequencePage(10, 20, 30, 40);
//        BuildSideSetup buildSideSetup = setupBuildSide(parallelBuild, taskContext, Ints.asList(0), buildPages, Optional.empty(), false, SINGLE_STREAM_SPILLER_FACTORY);
//        JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactory = buildSideSetup.getLookupSourceFactoryManager();

        // probe factory
        RowPagesBuilder probePages = rowPagesBuilder(probeHashEnabled, Ints.asList(0), ImmutableList.of(VARCHAR, BIGINT, BIGINT));
        List<Page> probeInput = probePages
                .addSequencePage(1000, 0, 1000, 2000)
                .build();
//        OperatorFactory joinOperatorFactory = innerJoinOperatorFactory(lookupSourceFactory, probePages, PARTITIONING_SPILLER_FACTORY);

        // build drivers and operators
//        instantiateBuildDrivers(buildSideSetup, taskContext);
//        buildLookupSource(buildSideSetup);

        // expected
        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), concat(probePages.getTypesWithoutHash(), buildPages.getTypesWithoutHash()))
                .row("20", 1020L, 2020L, "20", 30L, 40L)
                .row("21", 1021L, 2021L, "21", 31L, 41L)
                .row("22", 1022L, 2022L, "22", 32L, 42L)
                .row("23", 1023L, 2023L, "23", 33L, 43L)
                .row("24", 1024L, 2024L, "24", 34L, 44L)
                .row("25", 1025L, 2025L, "25", 35L, 45L)
                .row("26", 1026L, 2026L, "26", 36L, 46L)
                .row("27", 1027L, 2027L, "27", 37L, 47L)
                .row("28", 1028L, 2028L, "28", 38L, 48L)
                .row("29", 1029L, 2029L, "29", 39L, 49L)
                .build();

        assertOperatorEquals(joinOperatorFactory, taskContext.addPipelineContext(0, true, true, false).addDriverContext(), probeInput, expected, true, getHashChannels(probePages, buildPages));
    }

    public void testTableBasic(boolean parallelBuild, boolean probeHashEnabled, boolean buildHashEnabled)
    {
        TaskContext taskContext = createTaskContext();

        int length = 100;
        List<Type> types = ImmutableList.of(BIGINT, BIGINT,BIGINT);
        OptionalInt hashChannel = OptionalInt.of(types.size()-1);
        List<Integer> joinChannels = Ints.asList(0);
        List<Type> buildOutputTypes = types.subList(0,2);
        int expectedPositions = length;
        Optional<List<Integer>> buildOutputChannels = Optional.of(Ints.asList(0,1));
        JoinProbe.JoinProbeFactory buildJoinProbeFactory =  new JoinProbe.JoinProbeFactory(buildOutputChannels.get().stream().mapToInt(i -> i).toArray(), joinChannels, hashChannel);

        int partitionCount = parallelBuild ? PARTITION_COUNT : 1;

        AdaptiveJoinBridge joinBridge = new AdaptiveJoinBridge(
                types,hashChannel,joinChannels,buildOutputTypes,buildOutputChannels, TYPE_OPERATOR_FACTORY,length,LookupJoinOperatorFactory.JoinType.INNER,
                true,false,partitionCount);

        PartitionFunction buildPartitionFunction = getLocalPartitionGenerator(hashChannel,joinChannels,types,partitionCount);

        OperatorFactory hashBuildOperatorFactory = new HashBuildAndProbeOperator.HashBuildAndProbeOperatorFactory(
            0,0,buildPartitionFunction,joinBridge)
        )

        OperatorFactory outerJoinProcessingOperatorFactory = new OuterJoinResultProcessingOperator.OuterJoinResultProcessingOperatorFactory(
                0,0,joinBridge,true,buildPartitionFunction,
        )


        // build factory
        RowPagesBuilder buildPages = rowPagesBuilder(buildHashEnabled, Ints.asList(0), ImmutableList.of(BIGINT, BIGINT))
                .addSequencePage(10, 20, 30)
                .addSequencePage(10,30,40);


        List<Page> buildInput = buildPages.build();

        // probe factory
        RowPagesBuilder probePages = rowPagesBuilder(probeHashEnabled, Ints.asList(0), ImmutableList.of(BIGINT, BIGINT))
                .addSequencePage(10, 25, 1000)
                .addSequencePage(3,35,1000);


        List<Page> probeInput = probePages.build();

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

        List<Page> actual = new ArrayList<>();
        for (Page page: buildInput) {
            table.addPage(page);
        }
        for (Page page:probeInput) {
            actual.add(table.joinPage(page));
        }
        assertPagesEqualIgnoreOrder(taskContext.getSession(),actual,expected,false,Optional.empty());

        table.reset();

    }

    private LocalPartitionGenerator getLocalPartitionGenerator(OptionalInt hashChannel,List<Integer> joinChannels,List<Type> types,int partitionCount) {

        HashGenerator HashGenerator = null;
        requireNonNull(hashChannel, "probeHashChannel is null");
        if (hashChannel.isPresent()) {
            HashGenerator = new PrecomputedHashGenerator(hashChannel.getAsInt());
        }
        else {
            requireNonNull(joinChannels, "probeJoinChannels is null");
            List<Type> hashTypes = joinChannels.stream()
                    .map(types::get)
                    .collect(toImmutableList());
            HashGenerator = new InterpretedHashGenerator(hashTypes, joinChannels, blockTypeOperators);
        }
        return new LocalPartitionGenerator(HashGenerator,partitionCount);

    }

    private TaskContext createTaskContext()
    {
        return TestingTaskContext.createTaskContext(executor, scheduledExecutor, TEST_SESSION);
    }

    private static <T> List<T> concat(List<T> initialElements, List<T> moreElements)
    {
        return ImmutableList.copyOf(Iterables.concat(initialElements, moreElements));
    }
}
