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
package io.trino.sql.planner;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.Session;
import io.trino.metadata.Metadata;
import io.trino.operator.scalar.JsonPath;
import io.trino.plugin.base.expression.ConnectorExpressions;
import io.trino.security.AllowAllAccessControl;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.FieldDereference;
import io.trino.spi.expression.FunctionName;
import io.trino.spi.expression.StandardFunctions;
import io.trino.spi.expression.Variable;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.sql.tree.AstVisitor;
import io.trino.sql.tree.BinaryLiteral;
import io.trino.sql.tree.BooleanLiteral;
import io.trino.sql.tree.Cast;
import io.trino.sql.tree.CharLiteral;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.DecimalLiteral;
import io.trino.sql.tree.DoubleLiteral;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.LikePredicate;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.NotExpression;
import io.trino.sql.tree.NullLiteral;
import io.trino.sql.tree.StringLiteral;
import io.trino.sql.tree.SubscriptExpression;
import io.trino.sql.tree.LogicalBinaryExpression;
import io.trino.sql.tree.SymbolReference;
import io.trino.type.JoniRegexp;
import io.trino.type.JsonPathType;
import io.trino.type.Re2JRegexp;
import io.trino.type.Re2JRegexpType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.airlift.slice.SliceUtf8.countCodePoints;
import static io.trino.spi.expression.StandardFunctions.AND_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.CAST_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.GREATER_THAN_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.IS_DISTINCT_FROM_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.LESS_THAN_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.NOT_EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.NOT_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.OR_FUNCTION_NAME;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.sql.ExpressionUtils.combineConjuncts;
import static io.trino.sql.ExpressionUtils.extractConjuncts;
import static io.trino.sql.ExpressionUtils.isEffectivelyLiteral;
import static io.trino.sql.analyzer.TypeSignatureTranslator.toTypeSignature;
import static io.trino.sql.planner.ExpressionInterpreter.evaluateConstantExpression;
import static io.trino.type.JoniRegexpType.JONI_REGEXP;
import static java.util.Objects.requireNonNull;

public final class ConnectorExpressionTranslator
{
    private ConnectorExpressionTranslator() {}

    public static Expression translate(ConnectorExpression expression, Map<String, Symbol> variableMappings, LiteralEncoder literalEncoder)
    {
        return new ConnectorToSqlExpressionTranslator(variableMappings, literalEncoder).translate(expression);
    }

    public static Optional<ConnectorExpression> translate(Metadata metadata, Session session, Expression expression, TypeAnalyzer types, TypeProvider inputTypes)
    {
        return new SqlToConnectorExpressionTranslator(metadata, session, types.getTypes(session, inputTypes, expression))
                .process(expression);
    }

    public static ConnectorExpressionTranslation translateConjuncts(
            Metadata metadata,
            Session session,
            TypeProvider types,
            TypeAnalyzer typeAnalyzer,
            Expression expression)
    {
        SqlToConnectorExpressionTranslator translator = new SqlToConnectorExpressionTranslator(metadata, session, typeAnalyzer.getTypes(session, types, expression));

        List<Expression> conjuncts = extractConjuncts(expression);
        List<Expression> remaining = new ArrayList<>();
        List<ConnectorExpression> converted = new ArrayList<>(conjuncts.size());
        for (Expression conjunct : conjuncts) {
            Optional<ConnectorExpression> connectorExpression = translator.process(conjunct);
            if (connectorExpression.isPresent()) {
                converted.add(connectorExpression.get());
            }
            else {
                remaining.add(conjunct);
            }
        }
        return new ConnectorExpressionTranslation(
                ConnectorExpressions.and(converted),
                combineConjuncts(metadata, remaining));
    }

    @VisibleForTesting
    static FunctionName functionNameForComparisonOperator(ComparisonExpression.Operator operator)
    {
        switch (operator) {
            case EQUAL:
                return EQUAL_OPERATOR_FUNCTION_NAME;
            case NOT_EQUAL:
                return NOT_EQUAL_OPERATOR_FUNCTION_NAME;
            case LESS_THAN:
                return LESS_THAN_OPERATOR_FUNCTION_NAME;
            case LESS_THAN_OR_EQUAL:
                return LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME;
            case GREATER_THAN:
                return GREATER_THAN_OPERATOR_FUNCTION_NAME;
            case GREATER_THAN_OR_EQUAL:
                return GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME;
            case IS_DISTINCT_FROM:
                return IS_DISTINCT_FROM_OPERATOR_FUNCTION_NAME;
            default:
                throw new IllegalArgumentException("Unexpected value: " + operator);
        }
    }


    private static class ConnectorToSqlExpressionTranslator
    {
        private final Map<String, Symbol> variableMappings;
        private final LiteralEncoder literalEncoder;

        public ConnectorToSqlExpressionTranslator(Map<String, Symbol> variableMappings, LiteralEncoder literalEncoder)
        {
            this.variableMappings = requireNonNull(variableMappings, "variableMappings is null");
            this.literalEncoder = requireNonNull(literalEncoder, "literalEncoder is null");
        }

        public Expression translate(ConnectorExpression expression)
        {
            if (expression instanceof Variable) {
                return variableMappings.get(((Variable) expression).getName()).toSymbolReference();
            }

            if (expression instanceof Constant) {
                return literalEncoder.toExpression(((Constant) expression).getValue(), expression.getType());
            }

            if (expression instanceof FieldDereference) {
                FieldDereference dereference = (FieldDereference) expression;
                return new SubscriptExpression(translate(dereference.getTarget()), new LongLiteral(Long.toString(dereference.getField() + 1)));
            }

            throw new UnsupportedOperationException("Expression type not supported: " + expression.getClass().getName());
        }
    }

    public static class ConnectorExpressionTranslation
    {
        private final ConnectorExpression connectorExpression;
        private final Expression remainingExpression;

        public ConnectorExpressionTranslation(ConnectorExpression connectorExpression, Expression remainingExpression)
        {
            this.connectorExpression = requireNonNull(connectorExpression, "connectorExpression is null");
            this.remainingExpression = requireNonNull(remainingExpression, "remainingExpression is null");
        }

        public Expression getRemainingExpression()
        {
            return remainingExpression;
        }

        public ConnectorExpression getConnectorExpression()
        {
            return connectorExpression;
        }
    }

    static class SqlToConnectorExpressionTranslator
            extends AstVisitor<Optional<ConnectorExpression>, Void>
    {
        private final Session session;

        private final Metadata metadata;
        private final Map<NodeRef<Expression>, Type> types;

        public SqlToConnectorExpressionTranslator(Metadata metadata, Session session,Map<NodeRef<Expression>, Type> types)
        {
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.session = requireNonNull(session, "session is null");
            this.types = requireNonNull(types, "types is null");


        }

        @Override
        protected Optional<ConnectorExpression> visitSymbolReference(SymbolReference node, Void context)
        {
            return Optional.of(new Variable(node.getName(), typeOf(node)));
        }

        @Override
        protected Optional<ConnectorExpression> visitBooleanLiteral(BooleanLiteral node, Void context)
        {
            return Optional.of(new Constant(node.getValue(), typeOf(node)));
        }

        @Override
        protected Optional<ConnectorExpression> visitStringLiteral(StringLiteral node, Void context)
        {
            return Optional.of(new Constant(node.getSlice(), typeOf(node)));
        }

        @Override
        protected Optional<ConnectorExpression> visitDoubleLiteral(DoubleLiteral node, Void context)
        {
            return Optional.of(new Constant(node.getValue(), typeOf(node)));
        }

        @Override
        protected Optional<ConnectorExpression> visitDecimalLiteral(DecimalLiteral node, Void context)
        {
            return Optional.of(new Constant(Decimals.parse(node.getValue()).getObject(), typeOf(node)));
        }

        @Override
        protected Optional<ConnectorExpression> visitCharLiteral(CharLiteral node, Void context)
        {
            return Optional.of(new Constant(node.getSlice(), typeOf(node)));
        }

        @Override
        protected Optional<ConnectorExpression> visitBinaryLiteral(BinaryLiteral node, Void context)
        {
            return Optional.of(new Constant(node.getValue(), typeOf(node)));
        }

        @Override
        protected Optional<ConnectorExpression> visitLongLiteral(LongLiteral node, Void context)
        {
            return Optional.of(new Constant(node.getValue(), typeOf(node)));
        }

        @Override
        protected Optional<ConnectorExpression> visitNullLiteral(NullLiteral node, Void context)
        {
            return Optional.of(new Constant(null, typeOf(node)));
        }

        @Override
        protected Optional<ConnectorExpression> visitSubscriptExpression(SubscriptExpression node, Void context)
        {
            if (!(typeOf(node.getBase()) instanceof RowType)) {
                return Optional.empty();
            }

            Optional<ConnectorExpression> translatedBase = process(node.getBase());
            if (translatedBase.isEmpty()) {
                return Optional.empty();
            }

            return Optional.of(new FieldDereference(typeOf(node), translatedBase.get(), (int) (((LongLiteral) node.getIndex()).getValue() - 1)));
        }

        @Override
        protected Optional<ConnectorExpression> visitLikePredicate(LikePredicate node, Void context)
        {
            Optional<ConnectorExpression> value = process(node.getValue());
            Optional<ConnectorExpression> pattern = process(node.getPattern());
            if (value.isPresent() && pattern.isPresent()) {
                if (node.getEscape().isEmpty()) {
                    return Optional.of(new Call(typeOf(node), StandardFunctions.LIKE_FUNCTION_NAME, List.of(value.get(), pattern.get())));
                }

                Optional<ConnectorExpression> escape = process(node.getEscape().get());
                if (escape.isPresent()) {
                    return Optional.of(new Call(typeOf(node), StandardFunctions.LIKE_FUNCTION_NAME, List.of(value.get(), pattern.get(), escape.get())));
                }
            }
            return Optional.empty();
        }

        @Override
        protected Optional<ConnectorExpression> visitNotExpression(NotExpression node, Void context)
        {
            Optional<ConnectorExpression> translatedValue = process(node.getValue());
            if (translatedValue.isPresent()) {
                return Optional.of(new Call(BOOLEAN, NOT_FUNCTION_NAME, List.of(translatedValue.get())));
            }
            return Optional.empty();
        }

        @Override
        protected Optional<ConnectorExpression> visitComparisonExpression(ComparisonExpression node, Void context)
        {
            return process(node.getLeft()).flatMap(left -> process(node.getRight()).map(right ->
                    new Call(typeOf(node), functionNameForComparisonOperator(node.getOperator()), ImmutableList.of(left, right))));
        }

        @Override
        protected Optional<ConnectorExpression> visitCast(Cast node, Void context)
        {
            if (isEffectivelyLiteral(metadata, session, node)) {
                return Optional.of(constantFor(node));
            }

            if (node.isSafe()) {
                // try_cast would need to be modeled separately
                return Optional.empty();
            }


            Optional<ConnectorExpression> translatedExpression = process(node.getExpression());
            if (translatedExpression.isPresent()) {
                Type type = metadata.getType(toTypeSignature(node.getType()));
                return Optional.of(new Call(type, CAST_FUNCTION_NAME, List.of(translatedExpression.get())));
            }

            return Optional.empty();
        }

        private ConnectorExpression constantFor(Expression node)
        {
            Type type = typeOf(node);
            Object value = evaluateConstantExpression(
                    node,
                    type,
                    metadata,
                    session,
                    new AllowAllAccessControl(),
                    ImmutableMap.of());

            if (type == JONI_REGEXP) {
                Slice pattern = ((JoniRegexp) value).pattern();
                return new Constant(pattern, createVarcharType(countCodePoints(pattern)));
            }
            if (type instanceof Re2JRegexpType) {
                Slice pattern = Slices.utf8Slice(((Re2JRegexp) value).pattern());
                return new Constant(pattern, createVarcharType(countCodePoints(pattern)));
            }
            if (type instanceof JsonPathType) {
                Slice pattern = Slices.utf8Slice(((JsonPath) value).pattern());
                return new Constant(pattern, createVarcharType(countCodePoints(pattern)));
            }
            return new Constant(value, type);
        }

        @Override
        protected Optional<ConnectorExpression> visitLogicalBinaryExpression(LogicalBinaryExpression node, Void context)
        {
            ImmutableList.Builder<ConnectorExpression> arguments = ImmutableList.builderWithExpectedSize(2);
            Optional<ConnectorExpression> leftTranslated = process(node.getLeft());
            if (leftTranslated.isEmpty()) {
                return Optional.empty();
            }
            Optional<ConnectorExpression> rightTranslated = process(node.getRight());
            if (rightTranslated.isEmpty()) {
                return Optional.empty();
            }

            arguments.add(leftTranslated.get());
            arguments.add(rightTranslated.get());

            switch (node.getOperator()) {
                case AND:
                    return Optional.of(new Call(BOOLEAN, AND_FUNCTION_NAME, arguments.build()));
                case OR:
                    return Optional.of(new Call(BOOLEAN, OR_FUNCTION_NAME, arguments.build()));
                default:
                    return Optional.empty(); // or throw an exception, depending on your logic
            }

        }

        @Override
        protected Optional<ConnectorExpression> visitExpression(Expression node, Void context)
        {
            return Optional.empty();
        }

        private Type typeOf(Expression node)
        {
            return types.get(NodeRef.of(node));
        }
    }
}
