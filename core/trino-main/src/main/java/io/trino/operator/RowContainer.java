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

import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;

import java.util.Arrays;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.lang.Math.addExact;
import static java.lang.Math.max;
import static java.lang.Math.multiplyExact;
import static java.util.Objects.requireNonNull;

/**
 * RowContainer provides an append-only, row-wise in-memory representation of data for a given schema.
 *
 * The container is designed following the concepts of Velox's RowContainer and the flat row format
 * implemented in Trino's FlatHash. It uses a flat fixed-size region per row plus an optional
 * append-only variable-width area to store long values (e.g., strings, binary, complex types that
 * require variable bytes in their flat representation).
 *
 * - Schema: defined by a list of {@link Type} instances. The per-row fixed region size is computed as
 *   the sum of all field flat fixed sizes plus one null-flag byte per field. Variable-width bytes are
 *   stored out-of-line using {@link AppendOnlyVariableWidthData} when any field requires it.
 * - Append: rows are appended from {@link Block} arrays (e.g., from a Page), and a monotonic rowId is
 *   returned (0..size-1).
 * - Read: rows can be materialized back into {@link BlockBuilder}s or composed into a {@link Page}.
 * - Memory: memory is chunked into fixed-size groups for the fixed data and an append-only arena for
 *   variable data. The container tracks retained size for observability.
 *
 * This container supports logical deletion and full compaction to reclaim memory.
 */
public final class RowContainer
{
    public static final FlatHashStrategyCompiler TESTING_FLAT_HASH_STRATEGY_COMPILER = new FlatHashStrategyCompiler(new TypeOperators());

    private static final int INSTANCE_SIZE = instanceSize(RowContainer.class);

    // Use the same grouping as FlatHash so allocations stay reasonable and cache-friendly.
    private static final int RECORDS_PER_GROUP_SHIFT = 10; // 1024
    private static final int RECORDS_PER_GROUP = 1 << RECORDS_PER_GROUP_SHIFT;
    private static final int RECORDS_PER_GROUP_MASK = RECORDS_PER_GROUP - 1;

    private final List<Type> types; // original source order
    private final Type[] typesArray; // original order array
    private final Type[] layoutTypes; // internal layout order (keys first)
    private final int[] inputToLayout;
    private final int[] layoutToInput;
    private final FlatHashStrategy flatStrategy;

    private final boolean hasVariableData;
    private AppendOnlyVariableWidthData variableWidthData; // may be null if no variable-width types

    // Layout per record:
    //   [optional: pointer to variable chunk (int chunkIdx, int chunkOff)]
    //   [flat fixed bytes including per-field null flags]
    private final int variablePointerOffset;
    private final int fixedValueOffset;
    private final int fixedRecordSize;

    // Fixed-size records are chunked into groups to avoid large objects and ease partial releasing
    private byte[][] fixedSizeRecords; // array of record groups, each sized RECORDS_PER_GROUP * fixedRecordSize
    private int nextRowId;
    private int allocatedPhysicalSlots; // number of physical slots allocated at the tail (dense 0..allocated-1)
    private int aliveRowCount;

    // RowId -> physical location mapping and liveness
    private int[] rowIdToGroup;
    private int[] rowIdToIndexInGroup;
    private boolean[] rowIdAlive;

    // Free list of physical slots encoded as (group<<RECORDS_PER_GROUP_SHIFT) | indexInGroup
    private int[] freeSlots;
    private int freeSlotsSize;

    // Accounting
    private long fixedRecordGroupsRetainedSize;

    // Note: constructors requiring TypeOperators were removed to avoid implicit compiler creation.

    /**
     * Create a new RowContainer with a precompiled {@link FlatHashStrategy}. This is useful if callers
     * cache strategies.
     */
    public RowContainer(List<Type> types, FlatHashStrategy flatStrategy)
    {
        this.types = List.copyOf(requireNonNull(types, "types is null"));
        requireNonNull(flatStrategy, "flatStrategy is null");

        this.flatStrategy = flatStrategy;
        this.hasVariableData = flatStrategy.isAnyVariableWidth();
        this.variableWidthData = hasVariableData ? new AppendOnlyVariableWidthData() : null;

        // Record layout: [optional variable pointer] + [flat fixed bytes]
        this.variablePointerOffset = 0; // pointer (if present) is at start
        this.fixedValueOffset = hasVariableData ? AppendOnlyVariableWidthData.POINTER_SIZE : 0;
        this.fixedRecordSize = fixedValueOffset + flatStrategy.getTotalFlatFixedLength();

        this.typesArray = this.types.toArray(new Type[0]);
        this.layoutTypes = this.typesArray; // identity layout for this ctor
        this.inputToLayout = java.util.stream.IntStream.range(0, typesArray.length).toArray();
        this.layoutToInput = this.inputToLayout;
        // layout-related offsets are computed on the fly where needed

        // Allocate group array lazily to avoid large upfront allocations
        this.fixedSizeRecords = new byte[1][];
        this.nextRowId = 0;
        this.allocatedPhysicalSlots = 0;
        this.aliveRowCount = 0;
        this.rowIdToGroup = new int[0];
        this.rowIdToIndexInGroup = new int[0];
        this.rowIdAlive = new boolean[0];
        this.freeSlots = new int[0];
        this.freeSlotsSize = 0;
    }

    // Note: The constructor taking (types, keyChannels, TypeOperators) has been removed.

    // Note: key-channel grouping constructor removed; callers should pass a
    // FlatHashStrategy with the desired layout order.

    private boolean layoutHasVariable()
    {
        for (Type t : types) {
            if (t.isFlatVariableWidth()) {
                return true;
            }
        }
        return false;
    }

    public List<Type> getTypes()
    {
        return types;
    }

    List<Type> getLayoutTypes()
    {
        return List.of(layoutTypes);
    }

    int inputToLayoutIndex(int inputIndex)
    {
        return inputToLayout[inputIndex];
    }

    FlatHashStrategy getStrategy()
    {
        return flatStrategy;
    }

    public int size()
    {
        return aliveRowCount;
    }

    public long getRetainedSizeInBytes()
    {
        long result = INSTANCE_SIZE;
        result += sizeOf(fixedSizeRecords);
        result += fixedRecordGroupsRetainedSize;
        result += sizeOf(rowIdToGroup) + sizeOf(rowIdToIndexInGroup) + sizeOf(rowIdAlive) + sizeOf(freeSlots);
        result += (variableWidthData == null ? 0 : variableWidthData.getRetainedSizeBytes());
        return result;
    }

    /**
     * Append a single row from the provided blocks at the given position. The number of blocks must
     * match the schema size. Returns the assigned row id.
     */
    public int append(Block[] blocks, int position)
    {
        checkArgument(blocks.length == types.size(), "blocks do not match schema");

        final int rowId = nextRowId++;
        ensureRowIdCapacity(rowId + 1);

        // Determine physical slot
        int groupIndex;
        int indexInGroup;
        byte[] fixedRecords;
        int recordOffset;
        if (freeSlotsSize > 0) {
            int phys = freeSlots[--freeSlotsSize];
            groupIndex = phys >>> RECORDS_PER_GROUP_SHIFT;
            indexInGroup = phys & RECORDS_PER_GROUP_MASK;
            fixedRecords = fixedSizeRecords[groupIndex];
            recordOffset = indexInGroup * fixedRecordSize;
        }
        else {
            groupIndex = allocatedPhysicalSlots >> RECORDS_PER_GROUP_SHIFT;
            indexInGroup = allocatedPhysicalSlots & RECORDS_PER_GROUP_MASK;
            fixedSizeRecords = ensureCapacity(fixedSizeRecords, groupIndex + 1);
            fixedRecords = fixedSizeRecords[groupIndex];
            if (indexInGroup == 0) {
                if (fixedRecords != null) {
                    throw new IllegalStateException("fixedSizeRecords already exists for group");
                }
                fixedRecords = new byte[multiplyExact(RECORDS_PER_GROUP, fixedRecordSize)];
                fixedSizeRecords[groupIndex] = fixedRecords;
                fixedRecordGroupsRetainedSize = addExact(fixedRecordGroupsRetainedSize, sizeOf(fixedRecords));
            }
            recordOffset = indexInGroup * fixedRecordSize;
            allocatedPhysicalSlots++;
        }

        // Allocate variable-width bytes for this row if needed and write the pointer into the record
        byte[] variableChunk = null;
        int variableChunkOffset = 0;
        if (hasVariableData) {
            Block[] layoutBlocks = remapBlocksToLayout(blocks);
            int variableSize = flatStrategy.getTotalVariableWidth(layoutBlocks, position);
            variableChunk = variableWidthData.allocate(fixedRecords, recordOffset + variablePointerOffset, variableSize);
            variableChunkOffset = AppendOnlyVariableWidthData.getChunkOffset(fixedRecords, recordOffset + variablePointerOffset);
        }

        Block[] layoutBlocks = remapBlocksToLayout(blocks);
        flatStrategy.writeFlat(layoutBlocks, position, fixedRecords, recordOffset + fixedValueOffset, variableChunk, variableChunkOffset);

        // Map rowId to physical slot and mark alive
        rowIdToGroup[rowId] = groupIndex;
        rowIdToIndexInGroup[rowId] = indexInGroup;
        if (!rowIdAlive[rowId]) {
            rowIdAlive[rowId] = true;
            aliveRowCount++;
        }
        return rowId;
    }

    /**
     * Append all rows from a {@link Page}. Returns the starting row id of the
     * appended batch.
     */
    public int append(Page page)
    {
        checkArgument(page.getChannelCount() == types.size(), "page channels do not match schema");
        Block[] blocks = new Block[types.size()];
        for (int i = 0; i < blocks.length; i++) {
            blocks[i] = page.getBlock(i);
        }
        int start = nextRowId;
        for (int pos = 0; pos < page.getPositionCount(); pos++) {
            append(blocks, pos);
        }
        return start;
    }

    /**
     * Materialize the specified rows into the provided block builders.
     */
    public void appendTo(int rowId, BlockBuilder[] builders)
    {
        checkArgument(builders.length == types.size(), "builders do not match schema");
        checkArgument(rowId >= 0 && rowId < nextRowId, "rowId out of range");
        checkState(rowIdAlive[rowId], "rowId deleted");

        int recordGroupIndex = rowIdToGroup[rowId];
        byte[] fixedRecords = fixedSizeRecords[recordGroupIndex];
        int recordOffset = rowIdToIndexInGroup[rowId] * fixedRecordSize;

        byte[] variableChunk = null;
        int variableChunkOffset = 0;
        if (hasVariableData) {
            variableChunk = variableWidthData.getChunk(fixedRecords, recordOffset + variablePointerOffset);
            variableChunkOffset = AppendOnlyVariableWidthData.getChunkOffset(fixedRecords, recordOffset + variablePointerOffset);
        }

        BlockBuilder[] layoutBuilders = new BlockBuilder[layoutTypes.length];
        for (int j = 0; j < layoutBuilders.length; j++) {
            layoutBuilders[j] = builders[layoutToInput[j]];
        }
        flatStrategy.readFlat(fixedRecords, recordOffset + fixedValueOffset, variableChunk, variableChunkOffset, layoutBuilders);
    }

    /**
     * Build a Page containing the rows specified by rowIds, in order.
     */
    public Page toPage(int[] rowIds)
    {
        BlockBuilder[] builders = types.stream()
                .map(type -> type.createBlockBuilder(null, rowIds.length))
                .toArray(BlockBuilder[]::new);
        for (int rowId : rowIds) {
            appendTo(rowId, builders);
        }
        Block[] blocks = new Block[builders.length];
        for (int i = 0; i < builders.length; i++) {
            blocks[i] = builders[i].build();
        }
        return new Page(rowIds.length, blocks);
    }

    /**
     * Build a Page containing all rows currently stored, in append order.
     */
    public Page toPage()
    {
        return toPage(getAliveRowIds());
    }

    /**
     * Release fixed and variable-width memory for all rows strictly before the given rowId.
     * Subsequent reads for earlier rows are invalid.
     */
    public void releaseBefore(int rowId)
    {
        checkArgument(rowId >= 0 && rowId <= nextRowId, "rowId out of range");
        if (rowId == 0) {
            return;
        }
        int groupIndex = rowIdToGroup[rowId];
        // Free all complete groups before the group containing rowId
        for (int i = 0; i < groupIndex; i++) {
            byte[] released = fixedSizeRecords[i];
            if (released != null) {
                fixedSizeRecords[i] = null;
                fixedRecordGroupsRetainedSize -= sizeOf(released);
            }
        }
        if (hasVariableData) {
            int offsetInGroup = rowIdToIndexInGroup[rowId] * fixedRecordSize;
            byte[] group = fixedSizeRecords[groupIndex];
            if (group != null) {
                variableWidthData.freeChunksBefore(group, offsetInGroup + variablePointerOffset);
            }
        }
    }

    // Exposes low-level row pointer for efficient comparisons within the operator package
    static final class RowPointer
    {
        final byte[] fixed;
        final int fixedOffset; // start of fixed-value region for the row
        final byte[] variable; // may be null if schema has no variable-width types
        final int variableOffset; // base offset for variable-width region for this row

        RowPointer(byte[] fixed, int fixedOffset, byte[] variable, int variableOffset)
        {
            this.fixed = fixed;
            this.fixedOffset = fixedOffset;
            this.variable = variable;
            this.variableOffset = variableOffset;
        }
    }

    RowPointer getRowPointer(int rowId)
    {
        int recordGroupIndex = rowIdToGroup[rowId];
        byte[] fixedRecords = fixedSizeRecords[recordGroupIndex];
        int recordOffset = rowIdToIndexInGroup[rowId] * fixedRecordSize;

        byte[] variableChunk = null;
        int variableChunkOffset = 0;
        if (hasVariableData) {
            variableChunk = variableWidthData.getChunk(fixedRecords, recordOffset + variablePointerOffset);
            variableChunkOffset = AppendOnlyVariableWidthData.getChunkOffset(fixedRecords, recordOffset + variablePointerOffset);
        }

        return new RowPointer(fixedRecords, recordOffset + fixedValueOffset, variableChunk, variableChunkOffset);
    }

    public boolean delete(int rowId)
    {
        if (rowId < 0 || rowId >= nextRowId) {
            return false;
        }
        if (!rowIdAlive[rowId]) {
            return false;
        }
        int groupIndex = rowIdToGroup[rowId];
        int indexInGroup = rowIdToIndexInGroup[rowId];
        pushFree((groupIndex << RECORDS_PER_GROUP_SHIFT) | indexInGroup);
        rowIdAlive[rowId] = false;
        aliveRowCount--;
        return true;
    }

    /**
     * Compact the container by rewriting all live rows into fresh storage,
     * eliminating holes and rebuilding the variable-width arena.
     */
    public void compact()
    {
        if (aliveRowCount == 0) {
            // reset storage
            this.fixedSizeRecords = new byte[1][];
            this.fixedRecordGroupsRetainedSize = 0;
            this.allocatedPhysicalSlots = 0;
            this.freeSlots = new int[0];
            this.freeSlotsSize = 0;
            return;
        }

        // New storage sized for alive rows
        int groupsNeeded = Math.max(1, (aliveRowCount + RECORDS_PER_GROUP_MASK) >> RECORDS_PER_GROUP_SHIFT);
        byte[][] newFixed = new byte[groupsNeeded][];
        long newFixedRetained = 0;
        AppendOnlyVariableWidthData newVar = hasVariableData ? new AppendOnlyVariableWidthData() : null;

        int[] newRowToGroup = new int[nextRowId];
        int[] newRowToIndex = new int[nextRowId];

        int newAllocated = 0;
        for (int rowId = 0; rowId < nextRowId; rowId++) {
            if (!rowIdAlive[rowId]) {
                continue;
            }
            int dstGroup = newAllocated >> RECORDS_PER_GROUP_SHIFT;
            int dstIndex = newAllocated & RECORDS_PER_GROUP_MASK;
            newAllocated++;

            if (dstIndex == 0) {
                newFixed[dstGroup] = new byte[multiplyExact(RECORDS_PER_GROUP, fixedRecordSize)];
                newFixedRetained = addExact(newFixedRetained, sizeOf(newFixed[dstGroup]));
            }

            // source
            int srcGroup = rowIdToGroup[rowId];
            int srcIndex = rowIdToIndexInGroup[rowId];
            byte[] srcFixed = fixedSizeRecords[srcGroup];
            int srcRecordOffset = srcIndex * fixedRecordSize;

            // dest
            byte[] dstFixed = newFixed[dstGroup];
            int dstRecordOffset = dstIndex * fixedRecordSize;

            // move variable bytes
            if (hasVariableData) {
                int totalVar = computeTotalVariableLength(srcFixed, srcRecordOffset + fixedValueOffset);
                byte[] srcVarChunk = variableWidthData.getChunk(srcFixed, srcRecordOffset + variablePointerOffset);
                int srcVarOffset = AppendOnlyVariableWidthData.getChunkOffset(srcFixed, srcRecordOffset + variablePointerOffset);
                byte[] dstVarChunk = newVar.allocate(dstFixed, dstRecordOffset + variablePointerOffset, totalVar);
                int dstVarOffset = AppendOnlyVariableWidthData.getChunkOffset(dstFixed, dstRecordOffset + variablePointerOffset);
                System.arraycopy(srcVarChunk, srcVarOffset, dstVarChunk, dstVarOffset, totalVar);
            }

            // copy fixed values (including null flags)
            System.arraycopy(srcFixed, srcRecordOffset + fixedValueOffset, dstFixed, dstRecordOffset + fixedValueOffset, flatStrategy.getTotalFlatFixedLength());

            newRowToGroup[rowId] = dstGroup;
            newRowToIndex[rowId] = dstIndex;
        }

        // swap in new storage and mappings
        this.fixedSizeRecords = newFixed;
        this.fixedRecordGroupsRetainedSize = newFixedRetained;
        this.allocatedPhysicalSlots = aliveRowCount;
        if (hasVariableData) {
            // replace variable arena reference; previous arena becomes eligible for GC
            this.variableWidthData = newVar;
        }
        this.rowIdToGroup = newRowToGroup;
        this.rowIdToIndexInGroup = newRowToIndex;
        this.freeSlots = new int[0];
        this.freeSlotsSize = 0;
    }

    private int computeTotalVariableLength(byte[] fixed, int fixedBaseOffset)
    {
        int total = 0;
        int off = 0;
        for (int i = 0; i < layoutTypes.length; i++) {
            if (layoutTypes[i].isFlatVariableWidth()) {
                total += layoutTypes[i].getFlatVariableWidthLength(fixed, fixedBaseOffset + off + 1);
            }
            off += 1 + layoutTypes[i].getFlatFixedSize();
        }
        return total;
    }

    private void ensureRowIdCapacity(int min)
    {
        if (rowIdToGroup.length >= min) {
            return;
        }
        int newLen = Math.max(16, Integer.highestOneBit(min - 1) << 1);
        rowIdToGroup = Arrays.copyOf(rowIdToGroup, newLen);
        rowIdToIndexInGroup = Arrays.copyOf(rowIdToIndexInGroup, newLen);
        rowIdAlive = Arrays.copyOf(rowIdAlive, newLen);
    }

    private void pushFree(int phys)
    {
        if (freeSlotsSize == freeSlots.length) {
            int newLen = Math.max(8, freeSlots.length * 2);
            freeSlots = Arrays.copyOf(freeSlots, newLen);
        }
        freeSlots[freeSlotsSize++] = phys;
    }

    int[] getAliveRowIds()
    {
        int[] ids = new int[aliveRowCount];
        int pos = 0;
        for (int i = 0; i < nextRowId; i++) {
            if (rowIdAlive[i]) {
                ids[pos++] = i;
            }
        }
        return ids;
    }

    private static byte[][] ensureCapacity(byte[][] array, int minLength)
    {
        if (array.length >= minLength) {
            return array;
        }
        int newLength = max(array.length * 2, minLength);
        return Arrays.copyOf(array, newLength);
    }

    private Block[] remapBlocksToLayout(Block[] blocks)
    {
        if (layoutToInput == inputToLayout) {
            // identity mapping
            return blocks;
        }
        Block[] mapped = new Block[blocks.length];
        for (int j = 0; j < mapped.length; j++) {
            mapped[j] = blocks[layoutToInput[j]];
        }
        return mapped;
    }
}
