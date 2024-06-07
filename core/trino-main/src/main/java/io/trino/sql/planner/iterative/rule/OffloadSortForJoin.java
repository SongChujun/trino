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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.trino.Session;
import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.metadata.Metadata;
import io.trino.spi.connector.SortOrder;
import io.trino.split.SplitManager;
import io.trino.sql.analyzer.FeaturesConfig;
import io.trino.sql.planner.OrderingScheme;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.OffloadSortJoinNode;
import io.trino.sql.planner.plan.Patterns;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.SortMergeAdaptiveJoinNode;
import io.trino.sql.planner.plan.SortNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.tree.ComparisonExpression;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static io.trino.SystemSessionProperties.getElasticJoinRightPushdownRatio;
import static io.trino.SystemSessionProperties.getElasticJoinType;
import static io.trino.SystemSessionProperties.isAllowOffloadSortForJoinIntoConnectors;
import static io.trino.matching.Capture.newCapture;
import static io.trino.sql.planner.iterative.rule.PushJoinIntoTableScan.getFilterPredicates;
import static io.trino.sql.planner.plan.Patterns.Join.left;
import static io.trino.sql.planner.plan.Patterns.Join.right;
import static java.util.Objects.requireNonNull;

public class OffloadSortForJoin
        implements Rule<JoinNode>
{

    private static final Capture<PlanNode> LEFT = newCapture();

    private static final Capture<PlanNode> RIGHT = newCapture();

    private static final Capture<TableScanNode> RIGHT_TABLE_SCAN = newCapture();
    private static final Pattern<JoinNode> PATTERN =
            Patterns.join()
                    .with(left().capturedAs(LEFT))
                    .with(right().capturedAs(RIGHT));


    private final Metadata metadata;
    private final SplitManager splitManager;

    public OffloadSortForJoin(Metadata metadata)
    {
        this(metadata, null);
    }

    public OffloadSortForJoin(Metadata metadata, SplitManager splitManager)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
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
        return isAllowOffloadSortForJoinIntoConnectors(session);
    }

    @Override
    public Result apply(JoinNode joinNode, Captures captures, Context context)
    {
        if (joinNode.isCrossJoin()) {
            return Result.empty();
        }

        PlanNode left = captures.get(LEFT);
        PlanNode right = captures.get(RIGHT);

//        if (left instanceof TableScanNode && right instanceof TableScanNode) {
//            return Result.empty();
//        }
        if (!(left instanceof TableScanNode || right instanceof TableScanNode)) {
            return Result.empty();
        }

        if (left instanceof TableScanNode) {
            joinNode = joinNode.flipChildren();
            PlanNode temp = left;
            left = right;
            right = temp;
        }

        TableScanNode rightScan = (TableScanNode) right;


        verify(!rightScan.isUpdateTarget(), "Unexpected Join over for-update table scan");

        if (rightScan.getEnforcedConstraint().isNone()) {
            // bailing out on one of the tables empty; this is not interesting case which makes handling
            // enforced constraint harder below.
            return Result.empty();
        }

        FeaturesConfig.ElasticJoinType elasticJoinType = getElasticJoinType(context.getSession());

        if (elasticJoinType == FeaturesConfig.ElasticJoinType.OFFLOAD) {
            TableScanNode rightDownTableScanNode = new TableScanNode(context.getIdAllocator().getNextId(), rightScan.getTable(), rightScan.getOutputSymbols(), rightScan.getAssignments(), rightScan.getEnforcedConstraint(), rightScan.getStatistics(), false, rightScan.getUseConnectorNodePartitioning());

            List<JoinNode.EquiJoinClause> clauses = joinNode.getCriteria();

            List<Symbol> leftSymbols = Lists.transform(clauses, JoinNode.EquiJoinClause::getLeft);
            List<Symbol> rightSymbols = Lists.transform(clauses, JoinNode.EquiJoinClause::getRight);

            ImmutableMap.Builder<Symbol, SortOrder> leftOrderingBuilder = ImmutableMap.builder();
            ImmutableMap.Builder<Symbol, SortOrder> rightOrderingBuilder = ImmutableMap.builder();
            leftSymbols.forEach(symbol -> leftOrderingBuilder.put(symbol, SortOrder.ASC_NULLS_LAST));
            rightSymbols.forEach(symbol -> rightOrderingBuilder.put(symbol, SortOrder.ASC_NULLS_LAST));

            OrderingScheme rightOrderingScheme = new OrderingScheme(rightSymbols, rightOrderingBuilder.build());

            Optional<List<ComparisonExpression>> rightFilterPredicates = getFilterPredicates(metadata, context, rightDownTableScanNode, rightSymbols.get(0), 1 - getElasticJoinRightPushdownRatio(context.getSession()));

            if (rightFilterPredicates.isEmpty()) {
                return Result.empty();
            }

            PlanNode rightUp = new FilterNode(context.getIdAllocator().getNextId(), right, rightFilterPredicates.get().get(0));
            PlanNode rightDown = new FilterNode(context.getIdAllocator().getNextId(), rightDownTableScanNode, rightFilterPredicates.get().get(1));
            rightDown = new SortNode(context.getIdAllocator().getNextId(), rightDown, rightOrderingScheme, false);
            PlanNode offloadSortJoinNode = new OffloadSortJoinNode(
                    context.getIdAllocator().getNextId(),
                    joinNode.getType(),
                    SortMergeAdaptiveJoinNode.AdaptiveExecutionType.STATIC,
                    left,
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
            return Result.ofPlanNode(offloadSortJoinNode);
        } else if (elasticJoinType == FeaturesConfig.ElasticJoinType.DYNAMIC || elasticJoinType == FeaturesConfig.ElasticJoinType.STATIC_MULTIPLE_SPLITS) {
            List<JoinNode.EquiJoinClause> clauses = joinNode.getCriteria();

            List<Symbol> leftSymbols = Lists.transform(clauses, JoinNode.EquiJoinClause::getLeft);
            List<Symbol> rightSymbols = Lists.transform(clauses, JoinNode.EquiJoinClause::getRight);

            ImmutableMap.Builder<Symbol, SortOrder> leftOrderingBuilder = ImmutableMap.builder();
            ImmutableMap.Builder<Symbol, SortOrder> rightOrderingBuilder = ImmutableMap.builder();
            leftSymbols.forEach(symbol -> leftOrderingBuilder.put(symbol, SortOrder.ASC_NULLS_LAST));
            rightSymbols.forEach(symbol -> rightOrderingBuilder.put(symbol, SortOrder.ASC_NULLS_LAST));

            OrderingScheme rightOrderingScheme = new OrderingScheme(rightSymbols, rightOrderingBuilder.build());

            PlanNode rightDown = new SortNode(context.getIdAllocator().getNextId(), right, rightOrderingScheme, false);
            splitManager.registerColocateTableHandle(rightScan.getTable(), rightScan.getTable());
            if (elasticJoinType.equals(FeaturesConfig.ElasticJoinType.STATIC_MULTIPLE_SPLITS)) {
                splitManager.setPushDownRatio(getElasticJoinRightPushdownRatio(context.getSession()));
                splitManager.setSplitAssignmentPolicy(SplitManager.SplitAssignmentPolicy.STATIC);
            }
            else {
                splitManager.setSplitAssignmentPolicy(SplitManager.SplitAssignmentPolicy.DYNAMIC);
            }
            PlanNode offLoadSortMergeNode = new OffloadSortJoinNode(
                    context.getIdAllocator().getNextId(),
                    joinNode.getType(),
                    SortMergeAdaptiveJoinNode.AdaptiveExecutionType.DYNAMIC,
                    left,
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
        } else {
            return Result.empty();
        }
    }
}
