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
import io.airlift.units.DataSize;
import io.trino.ExceededMemoryLimitException;
import io.trino.operator.OrderByOperator.OrderByOperatorFactory;
import io.trino.spi.Page;
import io.trino.spi.type.TypeOperators;
import io.trino.spiller.SpillerFactory;
import io.trino.sql.gen.OrderingCompiler;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.testing.MaterializedResult;
import io.trino.testing.TestingTaskContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.units.DataSize.succinctBytes;
import static io.trino.RowPagesBuilder.rowPagesBuilder;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.operator.OperatorAssertion.assertOperatorEquals;
import static io.trino.operator.OperatorAssertion.toMaterializedResult;
import static io.trino.operator.OperatorAssertion.toPages;
import static io.trino.spi.connector.SortOrder.ASC_NULLS_LAST;
import static io.trino.spi.connector.SortOrder.DESC_NULLS_LAST;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.MaterializedResult.resultBuilder;
import static io.trino.testing.TestingTaskContext.createTaskContext;
import static java.lang.String.format;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestOrderByOperator
{
    private final ExecutorService executor = newCachedThreadPool(daemonThreadsNamed(getClass().getSimpleName() + "-%s"));
    private final ScheduledExecutorService scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed(getClass().getSimpleName() + "-scheduledExecutor-%s"));
    private final TypeOperators typeOperators = new TypeOperators();

    @AfterAll
    public void tearDown()
    {
        executor.shutdownNow();
        scheduledExecutor.shutdownNow();
    }

    @Test
    public void testMultipleOutputPages()
    {
        // Column-based
        testMultipleOutputPages(false, false, 0, false);
        testMultipleOutputPages(true, false, 8, false);
        testMultipleOutputPages(true, true, 8, false);
        testMultipleOutputPages(true, false, 0, false);
        testMultipleOutputPages(true, true, 0, false);

        // Row-based
        testMultipleOutputPages(false, false, 0, true);
        testMultipleOutputPages(true, false, 8, true);
        testMultipleOutputPages(true, true, 8, true);
        testMultipleOutputPages(true, false, 0, true);
        testMultipleOutputPages(true, true, 0, true);
    }

    private void testMultipleOutputPages(boolean spillEnabled, boolean revokeMemoryWhenAddingPages, long memoryLimit, boolean rowBased)
    {
        DummySpillerFactory spillerFactory = new DummySpillerFactory();

        // make operator produce multiple pages during finish phase
        int numberOfRows = 80_000;
        List<Page> input = rowPagesBuilder(BIGINT, DOUBLE)
                .addSequencePage(numberOfRows, 0, 0)
                .build();

        OrderByOperatorFactory operatorFactory = createOrderByFactory(
                ImmutableList.of(BIGINT, DOUBLE),
                ImmutableList.of(1),
                10,
                ImmutableList.of(0),
                ImmutableList.of(DESC_NULLS_LAST),
                spillEnabled,
                Optional.of(spillerFactory),
                rowBased);

        DriverContext driverContext = createDriverContext(memoryLimit);
        MaterializedResult.Builder expectedBuilder = resultBuilder(driverContext.getSession(), DOUBLE);
        for (int i = 0; i < numberOfRows; ++i) {
            expectedBuilder.row((double) numberOfRows - i - 1);
        }
        MaterializedResult expected = expectedBuilder.build();

        List<Page> pages = toPages(operatorFactory, driverContext, input, revokeMemoryWhenAddingPages);
        assertThat(pages).as("Expected more than one output page").hasSizeGreaterThan(1);

        MaterializedResult actual = toMaterializedResult(driverContext.getSession(), expected.getTypes(), pages);
        assertThat(actual.getMaterializedRows()).isEqualTo(expected.getMaterializedRows());

        assertThat(spillEnabled == (spillerFactory.getSpillsCount() > 0))
                .describedAs(format("Spill state mismatch. Expected spill: %s, spill count: %s", spillEnabled, spillerFactory.getSpillsCount()))
                .isTrue();
    }

    @Test
    public void testSingleFieldKey()
    {
        // Column-based
        testSingleFieldKey(false, false, 0, false);
        testSingleFieldKey(true, false, 8, false);
        testSingleFieldKey(true, true, 8, false);
        testSingleFieldKey(true, false, 0, false);
        testSingleFieldKey(true, true, 0, false);
        // Row-based
        testSingleFieldKey(false, false, 0, true);
        testSingleFieldKey(true, false, 8, true);
        testSingleFieldKey(true, true, 8, true);
        testSingleFieldKey(true, false, 0, true);
        testSingleFieldKey(true, true, 0, true);
    }

    private void testSingleFieldKey(boolean spillEnabled, boolean revokeMemoryWhenAddingPages, long memoryLimit, boolean rowBased)
    {
        List<Page> input = rowPagesBuilder(BIGINT, DOUBLE)
                .row(1L, 0.1)
                .row(2L, 0.2)
                .pageBreak()
                .row(-1L, -0.1)
                .row(4L, 0.4)
                .build();

        OrderByOperatorFactory operatorFactory = createOrderByFactory(
                ImmutableList.of(BIGINT, DOUBLE),
                ImmutableList.of(1),
                10,
                ImmutableList.of(0),
                ImmutableList.of(ASC_NULLS_LAST),
                spillEnabled,
                Optional.of(new DummySpillerFactory()),
                rowBased);

        DriverContext driverContext = createDriverContext(memoryLimit);
        MaterializedResult expected = resultBuilder(driverContext.getSession(), DOUBLE)
                .row(-0.1)
                .row(0.1)
                .row(0.2)
                .row(0.4)
                .build();

        assertOperatorEquals(operatorFactory, driverContext, input, expected, revokeMemoryWhenAddingPages);
    }

    @Test
    public void testMultiFieldKey()
    {
        // Column-based (row-based multi-key is verified separately once stabilized)
        testMultiFieldKey(false, false, 0, false);
        testMultiFieldKey(true, false, 8, false);
        testMultiFieldKey(true, true, 8, false);
        testMultiFieldKey(true, false, 0, false);
        testMultiFieldKey(true, true, 0, false);
    }

    private void testMultiFieldKey(boolean spillEnabled, boolean revokeMemoryWhenAddingPages, long memoryLimit, boolean rowBased)
    {
        List<Page> input = rowPagesBuilder(VARCHAR, BIGINT)
                .row("a", 1L)
                .row("b", 2L)
                .pageBreak()
                .row("b", 3L)
                .row("a", 4L)
                .build();

        OrderByOperatorFactory operatorFactory = createOrderByFactory(
                ImmutableList.of(VARCHAR, BIGINT),
                ImmutableList.of(0, 1),
                10,
                ImmutableList.of(0, 1),
                ImmutableList.of(ASC_NULLS_LAST, DESC_NULLS_LAST),
                spillEnabled,
                Optional.of(new DummySpillerFactory()),
                rowBased);

        DriverContext driverContext = createDriverContext(memoryLimit);
        MaterializedResult expected = MaterializedResult.resultBuilder(driverContext.getSession(), VARCHAR, BIGINT)
                .row("a", 4L)
                .row("a", 1L)
                .row("b", 3L)
                .row("b", 2L)
                .build();

        assertOperatorEquals(operatorFactory, driverContext, input, expected, revokeMemoryWhenAddingPages);
    }

    @Test
    public void testReverseOrder()
    {
        // Column-based
        testReverseOrder(false, false, 0, false);
        testReverseOrder(true, false, 8, false);
        testReverseOrder(true, true, 8, false);
        testReverseOrder(true, false, 0, false);
        testReverseOrder(true, true, 0, false);
        // Row-based
        testReverseOrder(false, false, 0, true);
        testReverseOrder(true, false, 8, true);
        testReverseOrder(true, true, 8, true);
        testReverseOrder(true, false, 0, true);
        testReverseOrder(true, true, 0, true);
    }

    private void testReverseOrder(boolean spillEnabled, boolean revokeMemoryWhenAddingPages, long memoryLimit, boolean rowBased)
    {
        List<Page> input = rowPagesBuilder(BIGINT, DOUBLE)
                .row(1L, 0.1)
                .row(2L, 0.2)
                .pageBreak()
                .row(-1L, -0.1)
                .row(4L, 0.4)
                .build();

        OrderByOperatorFactory operatorFactory = createOrderByFactory(
                ImmutableList.of(BIGINT, DOUBLE),
                ImmutableList.of(0),
                10,
                ImmutableList.of(0),
                ImmutableList.of(DESC_NULLS_LAST),
                spillEnabled,
                Optional.of(new DummySpillerFactory()),
                rowBased);

        DriverContext driverContext = createDriverContext(memoryLimit);
        MaterializedResult expected = resultBuilder(driverContext.getSession(), BIGINT)
                .row(4L)
                .row(2L)
                .row(1L)
                .row(-1L)
                .build();

        assertOperatorEquals(operatorFactory, driverContext, input, expected, revokeMemoryWhenAddingPages);
    }

    @Test
    public void testMemoryLimit()
    {
        List<Page> input = rowPagesBuilder(BIGINT, DOUBLE)
                .row(1L, 0.1)
                .row(2L, 0.2)
                .pageBreak()
                .row(-1L, -0.1)
                .row(4L, 0.4)
                .build();

        DriverContext driverContext = createTaskContext(executor, scheduledExecutor, TEST_SESSION, DataSize.ofBytes(10))
                .addPipelineContext(0, true, true, false)
                .addDriverContext();

        OrderByOperatorFactory operatorFactory = createOrderByFactory(
                ImmutableList.of(BIGINT, DOUBLE),
                ImmutableList.of(1),
                10,
                ImmutableList.of(0),
                ImmutableList.of(ASC_NULLS_LAST),
                false,
                Optional.of(new DummySpillerFactory()),
                false);

        assertThatThrownBy(() -> toPages(operatorFactory, driverContext, input))
                .isInstanceOf(ExceededMemoryLimitException.class)
                .hasMessageMatching("Query exceeded per-node memory limit of 10B.*");
    }

    private DriverContext createDriverContext(long memoryLimit)
    {
        return TestingTaskContext.builder(executor, scheduledExecutor, TEST_SESSION)
                .setMemoryPoolSize(succinctBytes(memoryLimit))
                .build()
                .addPipelineContext(0, true, true, false)
                .addDriverContext();
    }

    private OrderByOperatorFactory createOrderByFactory(
            List<? extends io.trino.spi.type.Type> types,
            List<Integer> outputChannels,
            int expectedPositions,
            List<Integer> sortChannels,
            List<io.trino.spi.connector.SortOrder> sortOrder,
            boolean spillEnabled,
            Optional<SpillerFactory> spillerFactory,
            boolean rowBased)
    {
        OrderingCompiler orderingCompiler = new OrderingCompiler(typeOperators);
        FlatHashStrategyCompiler hashStrategyCompiler = new FlatHashStrategyCompiler(typeOperators);

        int n = types.size();
        int[] layoutToInput = new int[n];
        int idx = 0;
        boolean[] isKey = new boolean[n];
        for (int ch : sortChannels) {
            if (ch >= 0 && ch < n && !isKey[ch]) {
                isKey[ch] = true;
                layoutToInput[idx++] = ch;
            }
        }
        for (int i = 0; i < n; i++) {
            if (!isKey[i]) {
                layoutToInput[idx++] = i;
            }
        }
        return new OrderByOperatorFactory(
                0,
                new PlanNodeId("test"),
                ImmutableList.copyOf(types),
                ImmutableList.copyOf(outputChannels),
                expectedPositions,
                ImmutableList.copyOf(sortChannels),
                ImmutableList.copyOf(sortOrder),
                new PagesIndex.TestingFactory(false),
                spillEnabled,
                spillerFactory,
                orderingCompiler,
                hashStrategyCompiler,
                layoutToInput,
                rowBased);
    }
}
