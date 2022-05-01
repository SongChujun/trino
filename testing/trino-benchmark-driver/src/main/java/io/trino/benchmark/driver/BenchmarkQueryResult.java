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
package io.trino.benchmark.driver;

import io.airlift.units.Duration;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class BenchmarkQueryResult
{
    public enum Status
    {
        PASS, FAIL
    }

    private static final Stat FAIL_STAT = new Stat(new double[0]);
    private static final List<Stat> FAIL_STAT_LIST = new ArrayList<>();

    public static BenchmarkQueryResult passResult(Suite suite, BenchmarkQuery benchmarkQuery, Stat wallTimeNanos, Stat processCpuTimeNanos, Stat queryCpuTimeNanos, List<Stat> subStageWallTimeNanos, List<Stat> subStageCpuTimeNanos)
    {
        return new BenchmarkQueryResult(suite, benchmarkQuery, Optional.empty(), Status.PASS, Optional.empty(), wallTimeNanos, processCpuTimeNanos, queryCpuTimeNanos, Optional.empty(), Optional.empty(), subStageWallTimeNanos, subStageCpuTimeNanos);
    }

    public static BenchmarkQueryResult passResult(Suite suite, BenchmarkQuery benchmarkQuery, BenchmarkQuery dynamicQuery, Stat wallTimeNanos, Stat processCpuTimeNanos, Stat queryCpuTimeNanos, Stat dynamicQueryWallTimeNanos, Stat dynamicQueryCPUTimeNanos, List<Stat> subStageWallTimeNanos, List<Stat> subStageCpuTimeNanos)
    {
        return new BenchmarkQueryResult(suite, benchmarkQuery, Optional.of(dynamicQuery), Status.PASS, Optional.empty(), wallTimeNanos, processCpuTimeNanos, queryCpuTimeNanos, Optional.of(dynamicQueryWallTimeNanos), Optional.of(dynamicQueryCPUTimeNanos), subStageWallTimeNanos, subStageCpuTimeNanos);
    }

    public static BenchmarkQueryResult failResult(Suite suite, BenchmarkQuery benchmarkQuery, String errorMessage)
    {
        return new BenchmarkQueryResult(suite, benchmarkQuery, Optional.empty(), Status.FAIL, Optional.of(errorMessage), FAIL_STAT, FAIL_STAT, FAIL_STAT, Optional.empty(), Optional.empty(), FAIL_STAT_LIST, FAIL_STAT_LIST);
    }

    private final Suite suite;
    private final BenchmarkQuery benchmarkQuery;

    private final Optional<BenchmarkQuery> dynamicQuery;
    private final Status status;
    private final Optional<String> errorMessage;
    private final Stat wallTimeNanos;
    private final Stat processCpuTimeNanos;
    private final Stat queryCpuTimeNanos;

    private final Optional<Stat> dynamicQueryWallTimeNanos;

    private final Optional<Stat> dynamicQueryCPUTimeNanos;
    private final List<Stat> subStageWallTimeNanos;
    private final List<Stat> subStageCpuTimeNanos;

    private BenchmarkQueryResult(
            Suite suite,
            BenchmarkQuery benchmarkQuery,
            Optional<BenchmarkQuery> dynamicQuery,
            Status status,
            Optional<String> errorMessage,
            Stat wallTimeNanos,
            Stat processCpuTimeNanos,
            Stat queryCpuTimeNanos,
            Optional<Stat> dynamicQueryWallTimeNanos,
            Optional<Stat> dynamicQueryCPUTimeNanos,
            List<Stat> subStageWallTimeNanos,
            List<Stat> subStageCpuTimeNanos)
    {
        this.suite = requireNonNull(suite, "suite is null");
        this.benchmarkQuery = requireNonNull(benchmarkQuery, "benchmarkQuery is null");
        this.dynamicQuery = requireNonNull(dynamicQuery, "dynamic query is null");
        this.status = requireNonNull(status, "status is null");
        this.errorMessage = requireNonNull(errorMessage, "errorMessage is null");
        this.wallTimeNanos = requireNonNull(wallTimeNanos, "wallTimeNanos is null");
        this.processCpuTimeNanos = requireNonNull(processCpuTimeNanos, "processCpuTimeNanos is null");
        this.queryCpuTimeNanos = requireNonNull(queryCpuTimeNanos, "queryCpuTimeNanos is null");
        this.subStageWallTimeNanos = requireNonNull(subStageWallTimeNanos, "subStageWallTimeNanos is null");
        this.subStageCpuTimeNanos = requireNonNull(subStageCpuTimeNanos, "subStageCpuTimeNanos is null");
        this.dynamicQueryWallTimeNanos = dynamicQueryWallTimeNanos;
        this.dynamicQueryCPUTimeNanos = dynamicQueryCPUTimeNanos;
    }

    public Suite getSuite()
    {
        return suite;
    }

    public BenchmarkQuery getBenchmarkQuery()
    {
        return benchmarkQuery;
    }

    public Status getStatus()
    {
        return status;
    }

    public Optional<String> getErrorMessage()
    {
        return errorMessage;
    }

    public Stat getWallTimeNanos()
    {
        return wallTimeNanos;
    }

    public Stat getProcessCpuTimeNanos()
    {
        return processCpuTimeNanos;
    }

    public Stat getQueryCpuTimeNanos()
    {
        return queryCpuTimeNanos;
    }

    public Optional<Stat> getDynamicQueryWallTimeNanos()
    {
        return dynamicQueryWallTimeNanos;
    }

    public Optional<Stat> getDynamicQueryCPUTimeNanos()
    {
        return dynamicQueryCPUTimeNanos;
    }

    public Optional<BenchmarkQuery> getDynamicQuery()
    {
        return dynamicQuery;
    }

    public List<Stat> getSubStageWallTimeNanos()
    {
        return subStageWallTimeNanos;
    }

    public List<Stat> getSubStageCpuTimeNanos()
    {
        return subStageCpuTimeNanos;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("suite", suite.getName())
                .add("benchmarkQuery", benchmarkQuery.getName())
                .add("status", status)
                .add("wallTimeMedian", new Duration(wallTimeNanos.getMedian(), NANOSECONDS).convertToMostSuccinctTimeUnit())
                .add("wallTimeMean", new Duration(wallTimeNanos.getMean(), NANOSECONDS).convertToMostSuccinctTimeUnit())
                .add("wallTimeStd", new Duration(wallTimeNanos.getStandardDeviation(), NANOSECONDS).convertToMostSuccinctTimeUnit())
                .add("processCpuTimeMedian", new Duration(processCpuTimeNanos.getMedian(), NANOSECONDS).convertToMostSuccinctTimeUnit())
                .add("processCpuTimeMean", new Duration(processCpuTimeNanos.getMean(), NANOSECONDS).convertToMostSuccinctTimeUnit())
                .add("processCpuTimeStd", new Duration(processCpuTimeNanos.getStandardDeviation(), NANOSECONDS).convertToMostSuccinctTimeUnit())
                .add("queryCpuTimeMedian", new Duration(queryCpuTimeNanos.getMedian(), NANOSECONDS).convertToMostSuccinctTimeUnit())
                .add("queryCpuTimeMean", new Duration(queryCpuTimeNanos.getMean(), NANOSECONDS).convertToMostSuccinctTimeUnit())
                .add("queryCpuTimeStd", new Duration(queryCpuTimeNanos.getStandardDeviation(), NANOSECONDS).convertToMostSuccinctTimeUnit())
                .add("error", errorMessage)
                .toString();
    }
}
