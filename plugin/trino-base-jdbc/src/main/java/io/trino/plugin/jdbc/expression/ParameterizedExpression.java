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
package io.trino.plugin.jdbc.expression;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.trino.plugin.jdbc.QueryParameter;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class ParameterizedExpression
{
    private final String expression;

    private final List<QueryParameter> parameters;

    @JsonCreator
    public ParameterizedExpression(
            @JsonProperty("expression") String expression,
            @JsonProperty("parameters") List<QueryParameter> parameters)
    {
        this.expression = requireNonNull(expression, "expression is null");
        this.parameters = ImmutableList.copyOf(requireNonNull(parameters, "parameters is null"));
    }

    @JsonProperty
    public String getExpression()
    {
        return expression;
    }

    @JsonProperty
    public List<QueryParameter> getParameters()
    {
        return parameters;
    }

}
