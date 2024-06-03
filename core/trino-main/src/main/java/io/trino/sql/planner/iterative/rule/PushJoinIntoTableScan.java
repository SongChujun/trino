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
package io.trino.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import io.trino.Session;
import io.trino.SystemSessionProperties;
import io.trino.cost.PlanNodeStatsEstimate;
import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.metadata.Metadata;
import io.trino.metadata.TableHandle;
import io.trino.metadata.TableSchema;
import io.trino.spi.connector.BasicRelationStatistics;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.JoinApplicationResult;
import io.trino.spi.connector.JoinCondition;
import io.trino.spi.connector.JoinStatistics;
import io.trino.spi.connector.JoinType;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.Variable;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.IntegerType;
import io.trino.split.SplitManager;
import io.trino.sql.ExpressionUtils;
import io.trino.sql.analyzer.FeaturesConfig;
import io.trino.sql.planner.OrderingScheme;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.TypeProvider;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.AdaptiveJoinNode;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.Patterns;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.SortMergeAdaptiveJoinNode;
import io.trino.sql.planner.plan.SortNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.tree.BooleanLiteral;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.SymbolReference;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.SystemSessionProperties.getElasticJoinLeftPushdownRatio;
import static io.trino.SystemSessionProperties.getElasticJoinRightPushdownRatio;
import static io.trino.SystemSessionProperties.getElasticJoinType;
import static io.trino.SystemSessionProperties.isAllowPushdownIntoConnectors;
import static io.trino.matching.Capture.newCapture;
import static io.trino.spi.predicate.Domain.onlyNull;
import static io.trino.sql.ExpressionUtils.and;
import static io.trino.sql.ExpressionUtils.extractConjuncts;
import static io.trino.sql.planner.iterative.rule.Rules.deriveTableStatisticsForPushdown;
import static io.trino.sql.planner.plan.JoinNode.Type.FULL;
import static io.trino.sql.planner.plan.JoinNode.Type.LEFT;
import static io.trino.sql.planner.plan.JoinNode.Type.RIGHT;
import static io.trino.sql.planner.plan.Patterns.Join.left;
import static io.trino.sql.planner.plan.Patterns.Join.right;
import static io.trino.sql.planner.plan.Patterns.tableScan;
import static java.lang.Double.isNaN;
import static java.util.Objects.requireNonNull;

public class PushJoinIntoTableScan
        implements Rule<JoinNode>
{
    private static final Capture<TableScanNode> LEFT_TABLE_SCAN = newCapture();
    private static final Capture<TableScanNode> RIGHT_TABLE_SCAN = newCapture();

    private static final Pattern<JoinNode> PATTERN =
            Patterns.join()
                    .with(left().matching(tableScan().capturedAs(LEFT_TABLE_SCAN)))
                    .with(right().matching(tableScan().capturedAs(RIGHT_TABLE_SCAN)));

    private final Metadata metadata;
    private final SplitManager splitManager;

    public PushJoinIntoTableScan(Metadata metadata)
    {
        this(metadata, null);
    }

    public PushJoinIntoTableScan(Metadata metadata, SplitManager splitManager)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
//        this.splitManager = requireNonNull(splitManager, "splitManager is null");
        this.splitManager = splitManager;
    }

    @Override
    public Pattern<JoinNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isAllowPushdownIntoConnectors(session);
    }

    @Override
    public Result apply(JoinNode joinNode, Captures captures, Context context)
    {
        if (joinNode.isCrossJoin()) {
            return Result.empty();
        }

        TableScanNode left = captures.get(LEFT_TABLE_SCAN);
        TableScanNode right = captures.get(RIGHT_TABLE_SCAN);

        verify(!left.isUpdateTarget() && !right.isUpdateTarget(), "Unexpected Join over for-update table scan");

        if (left.getEnforcedConstraint().isNone() || right.getEnforcedConstraint().isNone()) {
            // bailing out on one of the tables empty; this is not interesting case which makes handling
            // enforced constraint harder below.
            return Result.empty();
        }

        FeaturesConfig.JoinType joinImplementation = SystemSessionProperties.getJoinType(context.getSession());
        FeaturesConfig.ElasticJoinType elasticJoinType = getElasticJoinType(context.getSession());
        if (joinImplementation == FeaturesConfig.JoinType.HASH && (elasticJoinType == FeaturesConfig.ElasticJoinType.OFFLOAD || (elasticJoinType == FeaturesConfig.ElasticJoinType.DYNAMIC))) {
            elasticJoinType = FeaturesConfig.ElasticJoinType.OUTER;
        }

        if (elasticJoinType == FeaturesConfig.ElasticJoinType.OFFLOAD) {
            TableScanNode rightDownTableScanNode = new TableScanNode(context.getIdAllocator().getNextId(), right.getTable(), right.getOutputSymbols(), right.getAssignments(), right.getEnforcedConstraint(), right.getStatistics(), false, right.getUseConnectorNodePartitioning());
            TableScanNode leftDownTableScanNode = new TableScanNode(context.getIdAllocator().getNextId(), left.getTable(), left.getOutputSymbols(), left.getAssignments(), left.getEnforcedConstraint(), left.getStatistics(), false, left.getUseConnectorNodePartitioning());

            List<JoinNode.EquiJoinClause> clauses = joinNode.getCriteria();

            List<Symbol> leftSymbols = Lists.transform(clauses, JoinNode.EquiJoinClause::getLeft);
            List<Symbol> rightSymbols = Lists.transform(clauses, JoinNode.EquiJoinClause::getRight);

            ImmutableMap.Builder<Symbol, SortOrder> leftOrderingBuilder = ImmutableMap.builder();
            ImmutableMap.Builder<Symbol, SortOrder> rightOrderingBuilder = ImmutableMap.builder();
            leftSymbols.forEach(symbol -> leftOrderingBuilder.put(symbol, SortOrder.ASC_NULLS_LAST));
            rightSymbols.forEach(symbol -> rightOrderingBuilder.put(symbol, SortOrder.ASC_NULLS_LAST));

            OrderingScheme leftOrderingScheme = new OrderingScheme(leftSymbols, leftOrderingBuilder.build());
            OrderingScheme rightOrderingScheme = new OrderingScheme(rightSymbols, rightOrderingBuilder.build());

            Optional<List<ComparisonExpression>> leftFilterPredicates = getFilterPredicates(metadata, context, leftDownTableScanNode, leftSymbols.get(0), 1 - getElasticJoinLeftPushdownRatio(context.getSession()));
            Optional<List<ComparisonExpression>> rightFilterPredicates = getFilterPredicates(metadata, context, rightDownTableScanNode, rightSymbols.get(0), 1 - getElasticJoinLeftPushdownRatio(context.getSession()));

            if (leftFilterPredicates.isEmpty() || rightFilterPredicates.isEmpty()) {
                return Result.empty();
            }

            PlanNode leftUp = new FilterNode(context.getIdAllocator().getNextId(), left, leftFilterPredicates.get().get(0));
            PlanNode rightUp = new FilterNode(context.getIdAllocator().getNextId(), right, rightFilterPredicates.get().get(0));

            PlanNode leftDown = new FilterNode(context.getIdAllocator().getNextId(), leftDownTableScanNode, leftFilterPredicates.get().get(1));
            leftDown = new SortNode(context.getIdAllocator().getNextId(), leftDown, leftOrderingScheme, false);
            PlanNode rightDown = new FilterNode(context.getIdAllocator().getNextId(), rightDownTableScanNode, rightFilterPredicates.get().get(1));
            rightDown = new SortNode(context.getIdAllocator().getNextId(), rightDown, rightOrderingScheme, false);
            PlanNode offLoadSortMergeNode = new SortMergeAdaptiveJoinNode(
                    context.getIdAllocator().getNextId(),
                    joinNode.getType(),
                    SortMergeAdaptiveJoinNode.AdaptiveExecutionType.STATIC,
                    leftUp,
                    leftDown,
                    rightUp,
                    rightDown,
                    joinNode.getCriteria(),
                    joinNode.getLeftOutputSymbols(),
                    joinNode.getRightOutputSymbols(),
                    joinNode.isMaySkipOutputDuplicates(),
                    Optional.empty(),
                    joinNode.getLeftHashSymbol(),
                    joinNode.getRightHashSymbol(),
                    joinNode.getDistributionType(),
                    joinNode.isSpillable(),
                    joinNode.getDynamicFilters(),
                    joinNode.getReorderJoinStatsAndCost());
            return Result.ofPlanNode(offLoadSortMergeNode);
        }

        if (elasticJoinType == FeaturesConfig.ElasticJoinType.DYNAMIC || elasticJoinType == FeaturesConfig.ElasticJoinType.STATIC_MULTIPLE_SPLITS) {
            List<JoinNode.EquiJoinClause> clauses = joinNode.getCriteria();

            List<Symbol> leftSymbols = Lists.transform(clauses, JoinNode.EquiJoinClause::getLeft);
            List<Symbol> rightSymbols = Lists.transform(clauses, JoinNode.EquiJoinClause::getRight);

            ImmutableMap.Builder<Symbol, SortOrder> leftOrderingBuilder = ImmutableMap.builder();
            ImmutableMap.Builder<Symbol, SortOrder> rightOrderingBuilder = ImmutableMap.builder();
            leftSymbols.forEach(symbol -> leftOrderingBuilder.put(symbol, SortOrder.ASC_NULLS_LAST));
            rightSymbols.forEach(symbol -> rightOrderingBuilder.put(symbol, SortOrder.ASC_NULLS_LAST));

            OrderingScheme leftOrderingScheme = new OrderingScheme(leftSymbols, leftOrderingBuilder.build());
            OrderingScheme rightOrderingScheme = new OrderingScheme(rightSymbols, rightOrderingBuilder.build());

            PlanNode leftDown = new SortNode(context.getIdAllocator().getNextId(), left, leftOrderingScheme, false);
            PlanNode rightDown = new SortNode(context.getIdAllocator().getNextId(), right, rightOrderingScheme, false);
            splitManager.registerColocateTableHandle(left.getTable(), left.getTable());
            splitManager.registerColocateTableHandle(right.getTable(), right.getTable());
            if (elasticJoinType.equals(FeaturesConfig.ElasticJoinType.STATIC_MULTIPLE_SPLITS)) {
                splitManager.setPushDownRatio(getElasticJoinLeftPushdownRatio(context.getSession()));
                splitManager.setSplitAssignmentPolicy(SplitManager.SplitAssignmentPolicy.STATIC);
            }
            else {
                splitManager.setSplitAssignmentPolicy(SplitManager.SplitAssignmentPolicy.DYNAMIC);
            }
            PlanNode offLoadSortMergeNode = new SortMergeAdaptiveJoinNode(
                    context.getIdAllocator().getNextId(),
                    joinNode.getType(),
                    SortMergeAdaptiveJoinNode.AdaptiveExecutionType.DYNAMIC,
                    left,
                    leftDown,
                    right,
                    rightDown,
                    joinNode.getCriteria(),
                    joinNode.getLeftOutputSymbols(),
                    joinNode.getRightOutputSymbols(),
                    joinNode.isMaySkipOutputDuplicates(),
                    Optional.empty(),
                    joinNode.getLeftHashSymbol(),
                    joinNode.getRightHashSymbol(),
                    joinNode.getDistributionType(),
                    joinNode.isSpillable(),
                    joinNode.getDynamicFilters(),
                    joinNode.getReorderJoinStatsAndCost());
            return Result.ofPlanNode(offLoadSortMergeNode);
        }

        boolean useHybridJoin = false;

        ComparisonExpression buildFilterPredicate = null;

        ComparisonExpression outerFilterPredicate = null;
        Symbol rightFilterSymbol = null;
        LongLiteral rightDelimiter = null;
        List<ColumnHandle> leftPrimaryKeyColumnHandles = ImmutableList.of();
        List<Symbol> leftPrimaryKeySymbols = null;
        if (elasticJoinType == FeaturesConfig.ElasticJoinType.OUTER) {
            List<String> leftPrimaryKeyColumns = metadata.getPrimaryKeyColumns(context.getSession(), left.getTable());
            if (!leftPrimaryKeyColumns.isEmpty()) {
                List<String> rightPrimaryKeyColumns = metadata.getPrimaryKeyColumns(context.getSession(), right.getTable());
                if (!rightPrimaryKeyColumns.isEmpty()) {
                    Map<String, ColumnHandle> leftColumnHandles = metadata.getColumnHandles(context.getSession(), left.getTable());
                    Map<String, ColumnHandle> rightColumnHandles = metadata.getColumnHandles(context.getSession(), right.getTable());

                    Optional<ColumnHandle> firstRightPrimaryKeyColumnHandle = rightColumnHandles.entrySet().stream().filter(cl -> cl.getKey().equals(rightPrimaryKeyColumns.get(0))).map(Map.Entry::getValue).findFirst();
                    if (firstRightPrimaryKeyColumnHandle.isEmpty()) {
                        TableSchema rightTableSchema = metadata.getTableSchema(context.getSession(), right.getTable());
                        TableHandle rightTableHandle = metadata.getTableHandle(context.getSession(), rightTableSchema.getQualifiedName()).get();
                        Map<String, ColumnHandle> rightColumnHandlesAll = metadata.getColumnHandles(context.getSession(), rightTableHandle);
                        Map.Entry<String, ColumnHandle> firstRightPrimaryKeyEntry = rightColumnHandlesAll.entrySet().stream().filter(ch -> rightPrimaryKeyColumns.get(0).equals(ch.getKey())).findFirst().get();
                        ImmutableMap.Builder<Symbol, ColumnHandle> newRightAssignmentsBuilder = new ImmutableMap.Builder<>();
                        newRightAssignmentsBuilder.putAll(right.getAssignments());
                        ColumnMetadata columnMetadata = metadata.getColumnMetadata(context.getSession(), rightTableHandle, firstRightPrimaryKeyEntry.getValue());
                        Symbol newSymbol = context.getSymbolAllocator().newSymbol(firstRightPrimaryKeyEntry.getKey(), columnMetadata.getType());
                        newRightAssignmentsBuilder.put(newSymbol, firstRightPrimaryKeyEntry.getValue());
                        Map<Symbol, ColumnHandle> newRightAssignments = newRightAssignmentsBuilder.build();
                        right = new TableScanNode(
                                context.getIdAllocator().getNextId(),
                                rightTableHandle,
                                ImmutableList.copyOf(newRightAssignments.keySet()),
                                newRightAssignments,
                                right.getEnforcedConstraint(),
                                right.getStatistics(),
                                false,
                                right.getUseConnectorNodePartitioning());
                        firstRightPrimaryKeyColumnHandle = rightColumnHandlesAll.entrySet().stream().filter(cl -> cl.getKey().equals(rightPrimaryKeyColumns.get(0))).map(Map.Entry::getValue).findFirst();
                    }

                    List<String> leftNotIncludedPrimaryKeyStrs = leftPrimaryKeyColumns.stream().filter(str -> !leftColumnHandles.containsKey(str)).collect(toImmutableList());
                    if (!leftNotIncludedPrimaryKeyStrs.isEmpty()) {
                        TableSchema leftTableSchema = metadata.getTableSchema(context.getSession(), left.getTable());
                        TableHandle leftTableHandle = metadata.getTableHandle(context.getSession(), leftTableSchema.getQualifiedName()).get();
                        Map<String, ColumnHandle> leftColumnHandlesAll = metadata.getColumnHandles(context.getSession(), leftTableHandle);

                        Map<String, ColumnHandle> leftNotIncludedPrimaryKeys = leftColumnHandlesAll.entrySet().stream().filter(ch -> leftNotIncludedPrimaryKeyStrs.contains(ch.getKey())).collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
                        ImmutableMap.Builder<Symbol, ColumnHandle> newLeftAssignmentsBuilder = new ImmutableMap.Builder<>();
                        newLeftAssignmentsBuilder.putAll(left.getAssignments());
                        for (Map.Entry<String, ColumnHandle> entry : leftNotIncludedPrimaryKeys.entrySet()) {
                            ColumnMetadata columnMetadata = metadata.getColumnMetadata(context.getSession(), leftTableHandle, entry.getValue());
                            Symbol newSymbol = context.getSymbolAllocator().newSymbol(entry.getKey(), columnMetadata.getType());
                            newLeftAssignmentsBuilder.put(newSymbol, entry.getValue());
                        }
                        Map<Symbol, ColumnHandle> newLeftAssignments = newLeftAssignmentsBuilder.build();
                        left = new TableScanNode(
                                context.getIdAllocator().getNextId(),
                                leftTableHandle,
                                ImmutableList.copyOf(newLeftAssignments.keySet()),
                                newLeftAssignments,
                                left.getEnforcedConstraint(),
                                left.getStatistics(),
                                false,
                                left.getUseConnectorNodePartitioning());
                        leftPrimaryKeyColumnHandles = leftColumnHandlesAll.entrySet().stream().filter(cl -> leftPrimaryKeyColumns.contains(cl.getKey())).map(Map.Entry::getValue).collect(toImmutableList());
                    }
                    else {
                        leftPrimaryKeyColumnHandles = leftColumnHandles.entrySet().stream().filter(cl -> leftPrimaryKeyColumns.contains(cl.getKey())).map(Map.Entry::getValue).collect(toImmutableList());
                    }
                    List<ColumnHandle> finalLeftPrimaryKeyColumnHandles = leftPrimaryKeyColumnHandles;
                    leftPrimaryKeySymbols = left.getAssignments().entrySet().stream().filter(a -> finalLeftPrimaryKeyColumnHandles.contains(a.getValue())).map(Map.Entry::getKey).collect(toImmutableList());

                    double elasticJoinRightPushdownRatio = 1 - getElasticJoinRightPushdownRatio(context.getSession());
                    List<Object> nthPercentile = metadata.getNthPercentile(context.getSession(), right.getTable(), firstRightPrimaryKeyColumnHandle.get());
                    Optional<ColumnHandle> finalFirstRightPrimaryKeyColumnHandle = firstRightPrimaryKeyColumnHandle;
                    rightFilterSymbol = right.getAssignments().entrySet().stream().filter(entry -> entry.getValue().equals(finalFirstRightPrimaryKeyColumnHandle.get())).map(Map.Entry::getKey).findFirst().orElse(null);
                    if (rightFilterSymbol == null) {
                        throw new IllegalStateException();
                    }

                    Object minColumnVal = nthPercentile.get(0);
                    Object maxVColumnVal = nthPercentile.get(1);
                    if ((minColumnVal instanceof Integer)) {
                        int intMinColumnVal = (Integer) minColumnVal;
                        int intMaxColumnVal = (Integer) maxVColumnVal;
                        int delimiterVal = intMinColumnVal + (int) (elasticJoinRightPushdownRatio * (intMaxColumnVal - intMinColumnVal));
                        rightDelimiter = new LongLiteral(String.valueOf(delimiterVal));
                        useHybridJoin = true;
                        if (delimiterVal <= intMinColumnVal) {
                            useHybridJoin = false;
                        }
                        else if (delimiterVal >= intMaxColumnVal) {
                            return Result.empty();
                        }
                        else {
                            buildFilterPredicate = new ComparisonExpression(ComparisonExpression.Operator.GREATER_THAN, rightDelimiter, rightFilterSymbol.toSymbolReference());
                            outerFilterPredicate = new ComparisonExpression(ComparisonExpression.Operator.LESS_THAN_OR_EQUAL, rightDelimiter, rightFilterSymbol.toSymbolReference());
                        }
                    }
                    else {
                        useHybridJoin = false;
                    }
                }
            }
        }

        Expression effectiveFilter = getEffectiveFilter(joinNode);
        FilterSplitResult filterSplitResult = splitFilter(effectiveFilter, left.getOutputSymbols(), right.getOutputSymbols(), context);
        JoinCondition filterCondition;
        List<JoinCondition> joinConditions = filterSplitResult.getPushableConditions();
        if (useHybridJoin) {
            filterCondition = new JoinCondition(
                    joinConditionOperator(outerFilterPredicate.getOperator()),
                    new Constant(rightDelimiter.getValue(), IntegerType.INTEGER),
                    new Variable(rightFilterSymbol.getName(), context.getSymbolAllocator().getTypes().get(rightFilterSymbol)));
            joinConditions = new ImmutableList.Builder<JoinCondition>()
                    .addAll(joinConditions)
                    .add(filterCondition)
                    .build();
        }

        if (!filterSplitResult.getRemainingFilter().equals(BooleanLiteral.TRUE_LITERAL)) {
            // TODO add extra filter node above join
            return Result.empty();
        }

        Map<String, ColumnHandle> leftAssignments = left.getAssignments().entrySet().stream()
                .collect(toImmutableMap(entry -> entry.getKey().getName(), Map.Entry::getValue));

        Map<String, ColumnHandle> rightAssignments = right.getAssignments().entrySet().stream()
                .collect(toImmutableMap(entry -> entry.getKey().getName(), Map.Entry::getValue));

        /*
         * We are (lazily) computing estimated statistics for join node and left and right table
         * and passing those to connector via applyJoin.
         *
         * There are a couple reasons for this approach:
         * - the engine knows how to estimate join and connector may not
         * - the engine may have cached stats for the table scans (within context.getStatsProvider()), so can be able to provide information more inexpensively
         * - in the future, the engine may be able to provide stats for table scan even in case when connector no longer can (see https://github.com/trinodb/trino/issues/6998)
         * - the pushdown feasibility assessment logic may be different (or configured differently) for different connectors/catalogs.
         */
        JoinStatistics joinStatistics = getJoinStatistics(joinNode, left, right, context);

        Optional<JoinApplicationResult<TableHandle>> joinApplicationResult = metadata.applyJoin(
                context.getSession(),
                useHybridJoin ? JoinType.LEFT_OUTER : getJoinType(joinNode),
                left.getTable(),
                right.getTable(),
                joinConditions,
                (useHybridJoin && joinImplementation.equals(FeaturesConfig.JoinType.SORT_MERGE)) ? leftPrimaryKeyColumnHandles : ImmutableList.of(),
                // TODO we could pass only subset of assignments here, those which are needed to resolve filterSplitResult.getPushableConditions
                leftAssignments,
                rightAssignments,
                joinStatistics);

        if (joinApplicationResult.isEmpty()) {
            return Result.empty();
        }

        TableHandle handle = joinApplicationResult.get().getTableHandle();

        Map<ColumnHandle, ColumnHandle> leftColumnHandlesMapping = joinApplicationResult.get().getLeftColumnHandles();
        Map<ColumnHandle, ColumnHandle> rightColumnHandlesMapping = joinApplicationResult.get().getRightColumnHandles();

        ImmutableMap.Builder<Symbol, ColumnHandle> assignmentsBuilder = ImmutableMap.builder();
        assignmentsBuilder.putAll(left.getAssignments().entrySet().stream().collect(toImmutableMap(
                Map.Entry::getKey,
                entry -> leftColumnHandlesMapping.get(entry.getValue()))));
        assignmentsBuilder.putAll(right.getAssignments().entrySet().stream().collect(toImmutableMap(
                Map.Entry::getKey,
                entry -> rightColumnHandlesMapping.get(entry.getValue()))));
        Map<Symbol, ColumnHandle> assignments = assignmentsBuilder.build();

        // convert enforced constraint
        JoinNode.Type joinType = useHybridJoin ? LEFT : joinNode.getType();
        TupleDomain<ColumnHandle> leftConstraint = deriveConstraint(left.getEnforcedConstraint(), leftColumnHandlesMapping, joinType == RIGHT || joinType == FULL);
        TupleDomain<ColumnHandle> rightConstraint = deriveConstraint(right.getEnforcedConstraint(), rightColumnHandlesMapping, joinType == LEFT || joinType == FULL);

        TupleDomain<ColumnHandle> newEnforcedConstraint = TupleDomain.withColumnDomains(
                ImmutableMap.<ColumnHandle, Domain>builder()
                        // we are sure that domains map is present as we bailed out on isNone above
                        .putAll(leftConstraint.getDomains().orElseThrow())
                        .putAll(rightConstraint.getDomains().orElseThrow())
                        .build());
        // may need some extra condition here
        if (useHybridJoin) {
            PlanNode build = right;
            build = new FilterNode(context.getIdAllocator().getNextId(), build, buildFilterPredicate);
            PlanNode outer = new TableScanNode(
                    joinNode.getId(),
                    handle,
                    ImmutableList.copyOf(assignments.keySet()),
                    assignments,
                    newEnforcedConstraint,
                    deriveTableStatisticsForPushdown(context.getStatsProvider(), context.getSession(), joinApplicationResult.get().isPrecalculateStatistics(), joinNode),
                    false,
                    Optional.empty());
            return Result.ofPlanNode(
                    new AdaptiveJoinNode(
                            context.getIdAllocator().getNextId(),
                            joinNode.getType(),
                            build,
                            outer,
                            joinNode.getCriteria(),
                            joinNode.getRightOutputSymbols(),
                            joinNode.getLeftOutputSymbols(),
                            leftPrimaryKeySymbols,
                            // may hard code here!
                            left.getAssignments().keySet().stream().collect(toImmutableList()),
                            right.getAssignments().keySet().stream().collect(toImmutableList()),
                            joinNode.isMaySkipOutputDuplicates(),
                            joinNode.getFilter(),
                            Optional.empty(),
                            Optional.empty(),
                            joinNode.getDistributionType(),
                            joinNode.isSpillable(),
                            joinNode.getDynamicFilters(),
                            joinNode.getReorderJoinStatsAndCost()));
        }

        return Result.ofPlanNode(
                new ProjectNode(
                        context.getIdAllocator().getNextId(),
                        new TableScanNode(
                                joinNode.getId(),
                                handle,
                                ImmutableList.copyOf(assignments.keySet()),
                                assignments,
                                newEnforcedConstraint,
                                deriveTableStatisticsForPushdown(context.getStatsProvider(), context.getSession(), joinApplicationResult.get().isPrecalculateStatistics(), joinNode),
                                false,
                                Optional.empty()),
                        Assignments.identity(joinNode.getOutputSymbols())));
    }

    public static Optional<List<ComparisonExpression>> getFilterPredicates(Metadata metadata, Context context, TableScanNode tableScanNode, Symbol delimiterSymbol, double ratio)
    {
        ColumnHandle columnHandle = tableScanNode.getAssignments().get(delimiterSymbol);
        List<Object> columnMinMax = metadata.getNthPercentile(context.getSession(), tableScanNode.getTable(), columnHandle);
        ImmutableList.Builder<ComparisonExpression> res = new ImmutableList.Builder<>();
        Object minColumnVal = columnMinMax.get(0);
        Object maxColumnVal = columnMinMax.get(1);
        if ((minColumnVal instanceof Integer)) {
            int intMinColumnVal = (Integer) minColumnVal;
            int intMaxColumnVal = (Integer) maxColumnVal;
            int leftDelimiterVal = intMinColumnVal + (int) (ratio * (intMaxColumnVal - intMinColumnVal));
            LongLiteral delimiterLiteral = new LongLiteral(String.valueOf(leftDelimiterVal));
            if (ratio >= 1.0) {
                res.add(new ComparisonExpression(ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL, delimiterLiteral, delimiterSymbol.toSymbolReference()));
                res.add(new ComparisonExpression(ComparisonExpression.Operator.LESS_THAN, delimiterLiteral, delimiterSymbol.toSymbolReference()));
            }
            else {
                res.add(new ComparisonExpression(ComparisonExpression.Operator.GREATER_THAN, delimiterLiteral, delimiterSymbol.toSymbolReference()));
                res.add(new ComparisonExpression(ComparisonExpression.Operator.LESS_THAN_OR_EQUAL, delimiterLiteral, delimiterSymbol.toSymbolReference()));
            }
            return Optional.of(res.build());
        }
        else {
            return Optional.empty();
        }
    }

    private JoinStatistics getJoinStatistics(JoinNode join, TableScanNode left, TableScanNode right, Context context)
    {
        return new JoinStatistics()
        {
            @Override
            public Optional<BasicRelationStatistics> getLeftStatistics()
            {
                return getBasicRelationStats(left, left.getOutputSymbols(), context);
            }

            @Override
            public Optional<BasicRelationStatistics> getRightStatistics()
            {
                return getBasicRelationStats(right, right.getOutputSymbols(), context);
            }

            @Override
            public Optional<BasicRelationStatistics> getJoinStatistics()
            {
                return getBasicRelationStats(join, join.getOutputSymbols(), context);
            }

            private Optional<BasicRelationStatistics> getBasicRelationStats(PlanNode node, List<Symbol> outputSymbols, Context context)
            {
                PlanNodeStatsEstimate stats = context.getStatsProvider().getStats(node);
                TypeProvider types = context.getSymbolAllocator().getTypes();
                double outputRowCount = stats.getOutputRowCount();
                double outputSize = stats.getOutputSizeInBytes(outputSymbols, types);
                if (isNaN(outputRowCount) || isNaN(outputSize)) {
                    return Optional.empty();
                }
                return Optional.of(new BasicRelationStatistics((long) outputRowCount, (long) outputSize));
            }
        };
    }

    private TupleDomain<ColumnHandle> deriveConstraint(TupleDomain<ColumnHandle> sourceConstraint, Map<ColumnHandle, ColumnHandle> columnMapping, boolean nullable)
    {
        TupleDomain<ColumnHandle> constraint = sourceConstraint;
        if (nullable) {
            constraint = constraint.transformDomains((columnHandle, domain) -> domain.union(onlyNull(domain.getType())));
        }
        return constraint.transformKeys(columnMapping::get);
    }

    public Expression getEffectiveFilter(JoinNode node)
    {
        Expression effectiveFilter = and(node.getCriteria().stream().map(JoinNode.EquiJoinClause::toExpression).collect(toImmutableList()));
        if (node.getFilter().isPresent()) {
            effectiveFilter = and(effectiveFilter, node.getFilter().get());
        }
        return effectiveFilter;
    }

    private FilterSplitResult splitFilter(Expression filter, List<Symbol> leftSymbolsList, List<Symbol> rightSymbolsList, Context context)
    {
        Set<Symbol> leftSymbols = ImmutableSet.copyOf(leftSymbolsList);
        Set<Symbol> rightSymbols = ImmutableSet.copyOf(rightSymbolsList);

        ImmutableList.Builder<JoinCondition> comparisonConditions = ImmutableList.builder();
        ImmutableList.Builder<Expression> remainingConjuncts = ImmutableList.builder();

        for (Expression conjunct : extractConjuncts(filter)) {
            getPushableJoinCondition(conjunct, leftSymbols, rightSymbols, context)
                    .ifPresentOrElse(comparisonConditions::add, () -> remainingConjuncts.add(conjunct));
        }

        return new FilterSplitResult(comparisonConditions.build(), ExpressionUtils.and(remainingConjuncts.build()));
    }

    private Optional<JoinCondition> getPushableJoinCondition(Expression conjunct, Set<Symbol> leftSymbols, Set<Symbol> rightSymbols, Context context)
    {
        if (!(conjunct instanceof ComparisonExpression)) {
            return Optional.empty();
        }
        ComparisonExpression comparison = (ComparisonExpression) conjunct;

        if (!(comparison.getLeft() instanceof SymbolReference) || !(comparison.getRight() instanceof SymbolReference)) {
            return Optional.empty();
        }
        Symbol left = Symbol.from(comparison.getLeft());
        Symbol right = Symbol.from(comparison.getRight());
        ComparisonExpression.Operator operator = comparison.getOperator();

        if (!leftSymbols.contains(left)) {
            // lets try with flipped expression
            Symbol tmp = left;
            left = right;
            right = tmp;
            operator = operator.flip();
        }

        if (leftSymbols.contains(left) && rightSymbols.contains(right)) {
            return Optional.of(new JoinCondition(
                    joinConditionOperator(operator),
                    new Variable(left.getName(), context.getSymbolAllocator().getTypes().get(left)),
                    new Variable(right.getName(), context.getSymbolAllocator().getTypes().get(right))));
        }
        return Optional.empty();
    }

    private static class FilterSplitResult
    {
        private final List<JoinCondition> pushableConditions;
        private final Expression remainingFilter;

        public FilterSplitResult(List<JoinCondition> pushableConditions, Expression remainingFilter)
        {
            this.pushableConditions = requireNonNull(pushableConditions, "pushableConditions is null");
            this.remainingFilter = requireNonNull(remainingFilter, "remainingFilter is null");
        }

        public List<JoinCondition> getPushableConditions()
        {
            return pushableConditions;
        }

        public Expression getRemainingFilter()
        {
            return remainingFilter;
        }
    }

    private JoinCondition.Operator joinConditionOperator(ComparisonExpression.Operator operator)
    {
        switch (operator) {
            case EQUAL:
                return JoinCondition.Operator.EQUAL;
            case NOT_EQUAL:
                return JoinCondition.Operator.NOT_EQUAL;
            case LESS_THAN:
                return JoinCondition.Operator.LESS_THAN;
            case LESS_THAN_OR_EQUAL:
                return JoinCondition.Operator.LESS_THAN_OR_EQUAL;
            case GREATER_THAN:
                return JoinCondition.Operator.GREATER_THAN;
            case GREATER_THAN_OR_EQUAL:
                return JoinCondition.Operator.GREATER_THAN_OR_EQUAL;
            case IS_DISTINCT_FROM:
                return JoinCondition.Operator.IS_DISTINCT_FROM;
        }
        throw new IllegalArgumentException("Unknown operator: " + operator);
    }

    private JoinType getJoinType(JoinNode joinNode)
    {
        switch (joinNode.getType()) {
            case INNER:
                return JoinType.INNER;
            case LEFT:
                return JoinType.LEFT_OUTER;
            case RIGHT:
                return JoinType.RIGHT_OUTER;
            case FULL:
                return JoinType.FULL_OUTER;
        }
        throw new IllegalArgumentException("Unknown join type: " + joinNode.getType());
    }
}
