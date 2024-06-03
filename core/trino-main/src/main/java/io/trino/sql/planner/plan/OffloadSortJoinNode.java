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
package io.trino.sql.planner.plan;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.cost.PlanNodeStatsAndCostSummary;
import io.trino.sql.planner.Symbol;
import io.trino.sql.tree.Expression;

import javax.annotation.concurrent.Immutable;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.sql.planner.plan.JoinNode.DistributionType.REPLICATED;
import static io.trino.sql.planner.plan.JoinNode.Type.FULL;
import static io.trino.sql.planner.plan.JoinNode.Type.INNER;
import static io.trino.sql.planner.plan.JoinNode.Type.RIGHT;
import static java.util.Objects.requireNonNull;

@Immutable
public class OffloadSortJoinNode
        extends PlanNode
{
    private final JoinNode.Type type;

    private final SortMergeAdaptiveJoinNode.AdaptiveExecutionType adaptiveExecutionType;

    private final PlanNode rightUp;

    private final PlanNode rightDown;

    private final PlanNode left;
    private final List<JoinNode.EquiJoinClause> criteria;
    private final List<Symbol> leftOutputSymbols;
    private final List<Symbol> rightOutputSymbols;
    private final boolean maySkipOutputDuplicates;
    private final Optional<Expression> filter;
    private final Optional<Symbol> leftHashSymbol;
    private final Optional<Symbol> rightHashSymbol;
    private final Optional<JoinNode.DistributionType> distributionType;
    private final Optional<Boolean> spillable;
    private final Map<DynamicFilterId, Symbol> dynamicFilters;

    // stats and cost used for join reordering
    private final Optional<PlanNodeStatsAndCostSummary> reorderJoinStatsAndCost;

    @JsonCreator
    public OffloadSortJoinNode(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("type") JoinNode.Type type,
            @JsonProperty("adaptiveExecutionType") SortMergeAdaptiveJoinNode.AdaptiveExecutionType adaptiveExecutionType,
            @JsonProperty("left") PlanNode left,
            @JsonProperty("rightUp") PlanNode rightUp,
            @JsonProperty("rightDown") PlanNode rightDown,
            @JsonProperty("criteria") List<JoinNode.EquiJoinClause> criteria,
            @JsonProperty("leftOutputSymbols") List<Symbol> leftOutputSymbols,
            @JsonProperty("rightOutputSymbols") List<Symbol> rightOutputSymbols,
            @JsonProperty("maySkipOutputDuplicates") boolean maySkipOutputDuplicates,
            @JsonProperty("filter") Optional<Expression> filter,
            @JsonProperty("leftHashSymbol") Optional<Symbol> leftHashSymbol,
            @JsonProperty("rightHashSymbol") Optional<Symbol> rightHashSymbol,
            @JsonProperty("distributionType") Optional<JoinNode.DistributionType> distributionType,
            @JsonProperty("spillable") Optional<Boolean> spillable,
            @JsonProperty("dynamicFilters") Map<DynamicFilterId, Symbol> dynamicFilters,
            @JsonProperty("reorderJoinStatsAndCost") Optional<PlanNodeStatsAndCostSummary> reorderJoinStatsAndCost)
    {
        super(id);
        requireNonNull(type, "type is null");
        requireNonNull(adaptiveExecutionType, "adaptiveExecutionType is null");
        requireNonNull(left, "left is null");
        requireNonNull(rightUp, "rightUp is null");
        requireNonNull(rightDown, "rightDown is null");
        requireNonNull(criteria, "criteria is null");
        requireNonNull(leftOutputSymbols, "leftOutputSymbols is null");
        requireNonNull(rightOutputSymbols, "rightOutputSymbols is null");
        requireNonNull(filter, "filter is null");
        requireNonNull(leftHashSymbol, "leftHashSymbol is null");
        requireNonNull(rightHashSymbol, "rightHashSymbol is null");
        requireNonNull(distributionType, "distributionType is null");
        requireNonNull(spillable, "spillable is null");

        this.type = type;
        this.adaptiveExecutionType = adaptiveExecutionType;
        this.left = left;
        this.rightUp = rightUp;
        this.rightDown = rightDown;
        this.criteria = ImmutableList.copyOf(criteria);
        this.leftOutputSymbols = ImmutableList.copyOf(leftOutputSymbols);
        this.rightOutputSymbols = ImmutableList.copyOf(rightOutputSymbols);
        this.maySkipOutputDuplicates = maySkipOutputDuplicates;
        this.filter = filter;
        this.leftHashSymbol = leftHashSymbol;
        this.rightHashSymbol = rightHashSymbol;
        this.distributionType = distributionType;
        this.spillable = spillable;
        this.dynamicFilters = ImmutableMap.copyOf(requireNonNull(dynamicFilters, "dynamicFilters is null"));
        this.reorderJoinStatsAndCost = requireNonNull(reorderJoinStatsAndCost, "reorderJoinStatsAndCost is null");

        Set<Symbol> leftSymbols = ImmutableSet.copyOf(left.getOutputSymbols());
        Set<Symbol> rightSymbols = ImmutableSet.copyOf(rightUp.getOutputSymbols());

        checkArgument(leftSymbols.containsAll(leftOutputSymbols), "Left source inputs do not contain all left output symbols");
        checkArgument(rightSymbols.containsAll(rightOutputSymbols), "Right source inputs do not contain all right output symbols");

        checkArgument(!(criteria.isEmpty() && leftHashSymbol.isPresent()), "Left hash symbol is only valid in an equijoin");
        checkArgument(!(criteria.isEmpty() && rightHashSymbol.isPresent()), "Right hash symbol is only valid in an equijoin");

        criteria.forEach(equiJoinClause ->
                checkArgument(
                        leftSymbols.contains(equiJoinClause.getLeft()) &&
                                rightSymbols.contains(equiJoinClause.getRight()),
                        "Equality join criteria should be normalized according to join sides: %s", equiJoinClause));

        if (distributionType.isPresent()) {
            // The implementation of full outer join only works if the data is hash partitioned.
            checkArgument(
                    !(distributionType.get() == REPLICATED && (type == RIGHT || type == FULL)),
                    "%s join do not work with %s distribution type",
                    type,
                    distributionType.get());
        }

        for (Symbol symbol : dynamicFilters.values()) {
            checkArgument(rightSymbols.contains(symbol), "Right join input doesn't contain symbol for dynamic filter: %s", symbol);
        }
    }

    @JsonProperty("type")
    public JoinNode.Type getType()
    {
        return type;
    }

    @JsonProperty("adaptiveExecutionType")
    public SortMergeAdaptiveJoinNode.AdaptiveExecutionType getAdaptiveExecutionType()
    {
        return adaptiveExecutionType;
    }

    @JsonProperty("rightUp")
    public PlanNode getRightUp()
    {
        return rightUp;
    }

    @JsonProperty("rightDown")
    public PlanNode getRightDown()
    {
        return rightDown;
    }

    @JsonProperty("left")
    public PlanNode getLeft()
    {
        return left;
    }

    @JsonProperty("criteria")
    public List<JoinNode.EquiJoinClause> getCriteria()
    {
        return criteria;
    }

    @JsonProperty("leftOutputSymbols")
    public List<Symbol> getLeftOutputSymbols()
    {
        return leftOutputSymbols;
    }

    @JsonProperty("rightOutputSymbols")
    public List<Symbol> getRightOutputSymbols()
    {
        return rightOutputSymbols;
    }

    @JsonProperty("filter")
    public Optional<Expression> getFilter()
    {
        return filter;
    }

    @JsonProperty("leftHashSymbol")
    public Optional<Symbol> getLeftHashSymbol()
    {
        return leftHashSymbol;
    }

    @JsonProperty("rightHashSymbol")
    public Optional<Symbol> getRightHashSymbol()
    {
        return rightHashSymbol;
    }

    @Override
    public List<PlanNode> getSources()
    {
        return ImmutableList.of(left, rightUp, rightDown);
    }

    @Override
    public List<Symbol> getOutputSymbols()
    {
        return ImmutableList.<Symbol>builder()
                .addAll(leftOutputSymbols)
                .addAll(rightOutputSymbols)
                .build();
    }

    @JsonProperty("distributionType")
    public Optional<JoinNode.DistributionType> getDistributionType()
    {
        return distributionType;
    }

    @JsonProperty("spillable")
    public Optional<Boolean> isSpillable()
    {
        return spillable;
    }

    @JsonProperty("maySkipOutputDuplicates")
    public boolean isMaySkipOutputDuplicates()
    {
        return maySkipOutputDuplicates;
    }

    @JsonProperty
    public Map<DynamicFilterId, Symbol> getDynamicFilters()
    {
        return dynamicFilters;
    }

    @JsonProperty
    public Optional<PlanNodeStatsAndCostSummary> getReorderJoinStatsAndCost()
    {
        return reorderJoinStatsAndCost;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitOffloadSortJoin(this, context);
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        checkArgument(newChildren.size() == 3, "expected newChildren to contain 2 nodes");
        return new OffloadSortJoinNode(getId(), type, adaptiveExecutionType,  newChildren.get(0), newChildren.get(1), newChildren.get(2), criteria, leftOutputSymbols, rightOutputSymbols, maySkipOutputDuplicates, filter, leftHashSymbol, rightHashSymbol, distributionType, spillable, dynamicFilters, reorderJoinStatsAndCost);
    }


    public OffloadSortJoinNode withDistributionType(JoinNode.DistributionType distributionType)
    {
        return new OffloadSortJoinNode(getId(), type, adaptiveExecutionType, left, rightUp, rightDown, criteria, leftOutputSymbols, rightOutputSymbols, maySkipOutputDuplicates, filter, leftHashSymbol, rightHashSymbol, Optional.of(distributionType), spillable, dynamicFilters, reorderJoinStatsAndCost);
    }

    public boolean isCrossJoin()
    {
        return criteria.isEmpty() && filter.isEmpty() && type == INNER;
    }
}
