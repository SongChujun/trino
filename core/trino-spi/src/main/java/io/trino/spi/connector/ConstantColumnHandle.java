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
package io.trino.spi.connector;

import io.trino.spi.type.Type;

import java.util.Objects;

public final class ConstantColumnHandle
        implements ColumnHandle
{
    private final Type type;
    private final Object value;

    public ConstantColumnHandle(Object value, Type type)
    {
        this.value = value;
        this.type = type;
    }

    public Object getValue()
    {
        return value;
    }

    public Type getType()
    {
        return type;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(value, type);
    }

    @Override
    public boolean equals(Object other)
    {
        if (this == other) {
            return true;
        }
        if ((other == null) || (getClass() != other.getClass())) {
            return false;
        }
        ConstantColumnHandle o = (ConstantColumnHandle) other;
        return Objects.equals(this.value, o.value) && this.type.equals(o.type);
    }
}
