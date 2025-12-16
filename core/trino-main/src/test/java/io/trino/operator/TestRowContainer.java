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
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static io.trino.block.BlockAssertions.createRandomBlockForType;
import static io.trino.operator.PageAssertions.assertPageEquals;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

class TestRowContainer
{
    @Test
    void testRoundTripBasic()
    {
        List<Type> types = List.of(BIGINT, VARCHAR);
        int positions = 1_000;
        Block[] blocks = new Block[types.size()];
        for (int i = 0; i < types.size(); i++) {
            blocks[i] = createRandomBlockForType(types.get(i), positions, 0.1f);
        }
        Page input = new Page(positions, blocks);

        FlatHashStrategy flat = new FlatHashStrategyCompiler(new TypeOperators()).getFlatHashStrategy(types);
        RowContainer container = new RowContainer(types, flat);
        container.append(input);

        Page output = container.toPage();
        assertPageEquals(types, output, input);
        assertThat(container.size()).isEqualTo(positions);
    }

    @Test
    void testRoundTripWithVariableWidthAndSubset()
    {
        TypeOperators typeOperators = new TypeOperators();
        List<Type> types = ImmutableList.of(
                BIGINT,
                VARCHAR,
                VARBINARY,
                DOUBLE,
                new ArrayType(VARCHAR),
                new MapType(BIGINT, VARCHAR, typeOperators));

        int positions = 2573; // cross multiple groups
        Block[] blocks = new Block[types.size()];
        for (int i = 0; i < types.size(); i++) {
            blocks[i] = createRandomBlockForType(types.get(i), positions, 0.15f);
        }
        Page input = new Page(positions, blocks);

        FlatHashStrategy flat = new FlatHashStrategyCompiler(typeOperators).getFlatHashStrategy(types);
        RowContainer container = new RowContainer(types, flat);
        container.append(input);

        // Pick an arbitrary subset in a mixed order
        int outCount = 1000;
        int[] rowIds = new int[outCount];
        for (int i = 0; i < outCount; i++) {
            rowIds[i] = (i * 7) % positions; // pseudo-random but deterministic
        }

        Page output = container.toPage(rowIds);
        Page expected = materializeSubset(types, input, rowIds);
        assertPageEquals(types, output, expected);
    }

    @Test
    void testReleaseBefore()
    {
        List<Type> types = List.of(BIGINT, VARCHAR);
        int positions = 5_000;
        Block[] blocks = new Block[types.size()];
        for (int i = 0; i < types.size(); i++) {
            blocks[i] = createRandomBlockForType(types.get(i), positions, 0.05f);
        }
        Page input = new Page(positions, blocks);

        FlatHashStrategy flat2 = new FlatHashStrategyCompiler(new TypeOperators()).getFlatHashStrategy(types);
        RowContainer container = new RowContainer(types, flat2);
        container.append(input);

        int cutoff = 2049; // not aligned to group size
        container.releaseBefore(cutoff);

        int[] tailIds = new int[positions - cutoff];
        for (int i = 0; i < tailIds.length; i++) {
            tailIds[i] = cutoff + i;
        }
        Page output = container.toPage(tailIds);
        Page expected = materializeSubset(types, input, tailIds);
        assertPageEquals(types, output, expected);
    }

    @Test
    void testDeleteAndCompact()
    {
        List<Type> types = List.of(BIGINT, VARCHAR);
        int positions = 2048;
        Block[] blocks = new Block[types.size()];
        for (int i = 0; i < types.size(); i++) {
            blocks[i] = createRandomBlockForType(types.get(i), positions, 0.1f);
        }
        Page input = new Page(positions, blocks);

        FlatHashStrategy flat3 = new FlatHashStrategyCompiler(new TypeOperators()).getFlatHashStrategy(types);
        RowContainer container = new RowContainer(types, flat3);
        container.append(input);

        // delete every 3rd row
        int alive = positions;
        for (int id = 0; id < positions; id += 3) {
            if (container.delete(id)) {
                alive--;
            }
        }
        assertThat(container.size()).isEqualTo(alive);

        // round-trip matches expected subset
        int[] aliveIds = new int[alive];
        int idx = 0;
        for (int i = 0; i < positions; i++) {
            if (i % 3 != 0) {
                aliveIds[idx++] = i;
            }
        }
        Page expected = materializeSubset(types, input, aliveIds);
        Page beforeCompact = container.toPage();
        assertPageEquals(types, beforeCompact, expected);

        // compact and verify again
        container.compact();
        Page afterCompact = container.toPage();
        assertPageEquals(types, afterCompact, expected);
    }

    private static Page materializeSubset(List<Type> types, Page input, int[] rowIds)
    {
        List<Block> projected = new ArrayList<>(types.size());
        for (int c = 0; c < input.getChannelCount(); c++) {
            Block block = input.getBlock(c);
            projected.add(block.copyPositions(rowIds, 0, rowIds.length));
        }
        return new Page(rowIds.length, projected.toArray(Block[]::new));
    }
}
