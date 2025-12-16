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

import io.airlift.units.DataSize;
import io.trino.RowPagesBuilder;
import io.trino.spi.Page;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.gen.OrderingCompiler;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.testing.TestingTaskContext;
import org.junit.jupiter.api.Test;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.RunnerException;

import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.jmh.Benchmarks.benchmark;
import static io.trino.operator.RowContainer.TESTING_FLAT_HASH_STRATEGY_COMPILER;
import static io.trino.spi.connector.SortOrder.ASC_NULLS_FIRST;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Collections.nCopies;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.openjdk.jmh.annotations.Mode.AverageTime;

@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@BenchmarkMode(AverageTime)
@Fork(0)
@Warmup(iterations = 5, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 5, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@SuppressWarnings("MethodMayBeStatic")
public class BenchmarkOrderByOperator
{
    private static final Random RANDOM = new Random(633969769);
    private static final int ROWS_PER_PAGE = 1024;
    private static final int NUMBER_OF_PAGES = 100;

    @State(Scope.Thread)
    public static class BenchmarkContext
    {
        @Param({"2"})
        private String numberOfChannels = "1";

        @Param({"BIGINT"})
        private String typeName = "VARCHAR";

        // COLUMN = legacy PagesIndex-based path, ROW = new RowContainer-based path
        @Param({"ROW", "COLUMN"})
        private String mode = "ROW";

        private ExecutorService executor;
        private ScheduledExecutorService scheduledExecutor;
        private List<Type> types;
        private List<Integer> sortChannels;
        private List<SortOrder> sortOrders;
        private List<Page> pages;

        @Setup
        public void setup()
        {
            executor = newCachedThreadPool(daemonThreadsNamed(getClass().getSimpleName() + "-%s"));
            scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed(getClass().getSimpleName() + "-scheduledExecutor-%s"));

            int channels = Integer.parseInt(numberOfChannels);
            types = buildTypes(typeName, channels);
            sortChannels = java.util.stream.IntStream.range(0, channels).boxed().collect(toImmutableList());
            sortOrders = nCopies(channels, ASC_NULLS_FIRST);

            RowPagesBuilder pagesBuilder = RowPagesBuilder.rowPagesBuilder(types);
            for (int page = 0; page < NUMBER_OF_PAGES; page++) {
                for (int row = 0; row < ROWS_PER_PAGE; row++) {
                    Object[] values = new Object[channels];
                    for (int c = 0; c < channels; c++) {
                        Type t = types.get(c);
                        if (t == BIGINT) {
                            values[c] = RANDOM.nextLong(0, 100);
                        }
                        else if (t == VARCHAR) {
                            values[c] = io.airlift.slice.Slices.utf8Slice(String.valueOf(RANDOM.nextLong()));
                        }
                        else {
                            throw new IllegalArgumentException("Unsupported type: " + typeName);
                        }
                    }
                    pagesBuilder.row(values);
                }
                pagesBuilder.pageBreak();
            }
            pages = pagesBuilder.build();
        }

        private static List<Type> buildTypes(String typeName, int channels)
        {
            Type type = switch (typeName) {
                case "BIGINT" -> BIGINT;
                case "VARCHAR" -> VARCHAR;
                default -> throw new IllegalArgumentException("Unsupported type name: " + typeName);
            };
            return nCopies(channels, type);
        }

        public TaskContext createTaskContext()
        {
            return TestingTaskContext.createTaskContext(executor, scheduledExecutor, TEST_SESSION, DataSize.of(20, DataSize.Unit.GIGABYTE));
        }

        public void cleanup()
        {
            executor.shutdownNow();
            scheduledExecutor.shutdownNow();
        }
    }

    @Benchmark
    public int orderBy(BenchmarkContext context)
    {
        boolean useRowBased = context.mode.equals("ROW");

        OrderingCompiler orderingCompiler = new OrderingCompiler(new TypeOperators());
        FlatHashStrategyCompiler hashStrategyCompiler = new FlatHashStrategyCompiler(new TypeOperators());

        int n = context.types.size();
        int[] layoutToInput = new int[n];
        int idx = 0;
        boolean[] isKey = new boolean[n];
        for (int ch : context.sortChannels) {
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
        OrderByOperator.OrderByOperatorFactory factory = new OrderByOperator.OrderByOperatorFactory(
                0,
                new PlanNodeId("bench"),
                context.types,
                // output all channels
                java.util.stream.IntStream.range(0, context.types.size()).boxed().collect(toImmutableList()),
                ROWS_PER_PAGE * NUMBER_OF_PAGES,
                context.sortChannels,
                context.sortOrders,
                new PagesIndex.TestingFactory(false),
                false,
                java.util.Optional.of(new DummySpillerFactory()),
                orderingCompiler,
                TESTING_FLAT_HASH_STRATEGY_COMPILER,
                layoutToInput,
                useRowBased);

        DriverContext driverContext = context.createTaskContext().addPipelineContext(0, true, true, false).addDriverContext();
        Operator operator = factory.createOperator(driverContext);

        int outPositions = 0;
        java.util.Iterator<Page> input = context.pages.iterator();
        boolean finishing = false;
        for (int loops = 0; !operator.isFinished() && loops < 10_000_000; loops++) {
            if (operator.needsInput()) {
                if (input.hasNext()) {
                    operator.addInput(input.next());
                }
                else if (!finishing) {
                    operator.finish();
                    finishing = true;
                }
            }
            Page out = operator.getOutput();
            if (out != null) {
                outPositions += out.getPositionCount();
            }
        }

        return outPositions;
    }

    @Test
    public void verify()
    {
        BenchmarkContext context = new BenchmarkContext();
        context.setup();
        try {
            int positions = orderBy(context);
            org.assertj.core.api.Assertions.assertThat(positions).isEqualTo(ROWS_PER_PAGE * NUMBER_OF_PAGES);
        }
        finally {
            context.cleanup();
        }
    }

    public static void main(String[] args)
            throws RunnerException
    {
        BenchmarkContext data = new BenchmarkContext();
        data.setup();
        new BenchmarkOrderByOperator().orderBy(data);

        benchmark(BenchmarkOrderByOperator.class).run();
        data.cleanup();
    }
}
