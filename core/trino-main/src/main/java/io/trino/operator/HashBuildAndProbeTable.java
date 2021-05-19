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
import io.airlift.units.DataSize;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.type.Type;
import it.unimi.dsi.fastutil.HashCommon;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import org.openjdk.jol.info.ClassLayout;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.trino.operator.LookupJoinOperators.JoinType.FULL_OUTER;
import static io.trino.operator.LookupJoinOperators.JoinType.PROBE_OUTER;
import static io.trino.operator.SyntheticAddress.decodePosition;
import static io.trino.operator.SyntheticAddress.decodeSliceIndex;
import static io.trino.operator.SyntheticAddress.encodeSyntheticAddress;
import static io.trino.spi.StandardErrorCode.GENERIC_INSUFFICIENT_RESOURCES;
import static io.trino.util.HashCollisionsEstimator.estimateNumberOfHashCollisions;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

// This implementation assumes arrays used in the hash are always a power of 2
public final class HashBuildAndProbeTable implements LookupSource
{
    private class JoinProcessor {
        private List<Type> buildOutputTypes;
        private final JoinProbe.JoinProbeFactory joinProbeFactory;
        private LookupJoinPageBuilder pageBuilder;
        private IntArrayList probeIndexBuilder;

        private boolean currentProbePositionProducedRow;
        private boolean isSequentialProbeIndices;
        private int estimatedProbeBlockBytes;
        private final PageBuilder buildPageBuilder;
        private long joinPosition = -1;
        private int joinSourcePositions;
        private boolean outputSingleMatch;
        private final boolean probeOnOuterSide;
        private JoinProbe probe;
        private final JoinStatisticsCounter statisticsCounter;






        private JoinProcessor (List<Type> buildOutputTypes,
                JoinProbe.JoinProbeFactory joinProbeFactory,
                Optional<JoinFilterFunction> filterFunction,
                List<Type> buildTypes,
                LookupJoinOperators.JoinType joinType,
                boolean outputSingleMatch,
                JoinStatisticsCounter statisticsCounter
                )
        {
            this.buildOutputTypes = buildOutputTypes;
            this.joinProbeFactory = joinProbeFactory;
            this.pageBuilder =  new LookupJoinPageBuilder(buildOutputTypes);
            this.probeIndexBuilder = new IntArrayList();
            this.currentProbePositionProducedRow = false;
            this.isSequentialProbeIndices = false;
            this.estimatedProbeBlockBytes = 0;
            this.buildPageBuilder = new PageBuilder(requireNonNull(buildTypes, "buildTypes is null"));
            this.outputSingleMatch = outputSingleMatch;
            this.statisticsCounter = statisticsCounter;

            probeOnOuterSide = joinType == PROBE_OUTER || joinType == FULL_OUTER;


        }
        public Page joinPage(Page page) {

            probe = joinProbeFactory.createJoinProbe(page);
            do {
                if (probe.getPosition() >= 0) {
                    if (!joinCurrentPosition(HashBuildAndProbeTable.this)) {
                        break;
                    }
                    if (!currentProbePositionProducedRow) {
                        currentProbePositionProducedRow = true;
                        if (!outerJoinCurrentPosition()) {
                            break;
                        }
                    }
                    statisticsCounter.recordProbe(joinSourcePositions);
                }
                if (!advanceProbePosition(HashBuildAndProbeTable.this)) {
                    break;
                }
            } while (true);
            Page outputPage = pageBuilder.build(probe);
            pageBuilder.reset();
            return outputPage;
        }

        private boolean joinCurrentPosition(LookupSource lookupSource)
        {
            while (joinPosition >= 0) {
                if (lookupSource.isJoinPositionEligible(joinPosition, probe.getPosition(), probe.getPage())) {
                    currentProbePositionProducedRow = true;

                    pageBuilder.appendRow(probe, lookupSource, joinPosition);
                    joinSourcePositions++;
                }

                if (outputSingleMatch && currentProbePositionProducedRow) {
                    joinPosition = -1;
                }
                else {
                    // get next position on lookup side for this probe row
                    joinPosition = lookupSource.getNextJoinPosition(joinPosition, probe.getPosition(), probe.getPage());
                }

                if (pageBuilder.isFull()) {
                    return false;
                }
            }
            return true;
        }


        private boolean outerJoinCurrentPosition()
        {
            if (probeOnOuterSide) {
                pageBuilder.appendNullForBuild(probe);
                return !pageBuilder.isFull();
            }
            return true;
        }

        private boolean advanceProbePosition(HashBuildAndProbeTable lookupSource)
        {
            if (!probe.advanceNextPosition()) {
                return false;
            }

            // update join position
            joinPosition = probe.getCurrentJoinPosition(lookupSource);
            // reset row join state for next row
            joinSourcePositions = 0;
            currentProbePositionProducedRow = false;
            return true;
        }

        private int getEstimatedProbeRowSize(JoinProbe probe)
        {
            return 0;
        }


    }
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(PagesHash.class).instanceSize();
    private static final DataSize CACHE_SIZE = DataSize.of(128, KILOBYTE);
    private final ObjectArrayList<Block>[] channels;
    private final IntArrayList positionCounts;
    private int pageCount;
    private int positionCount;
    private final boolean eagerCompact;
    private long pagesMemorySize;
    private long estimatedSize;
    private List<Type> types;
    @Nullable
    private final JoinFilterFunction filterFunction;



    private PositionLinks.FactoryBuilder positionLinks;

    private final JoinProbe.JoinProbeFactory joinProbeFactory;
    private List<Type> buildOutputTypes;
    private final LongArrayList addresses;
    private final PagesHashStrategy pagesHashStrategy;

    private final int channelCount;
    private final int mask;
    private final int[] key;
    private long size;
    private final int hashSize;

    // Native array of hashes for faster collisions resolution compared
    // to accessing values in blocks. We use bytes to reduce memory foot print
    // and there is no performance gain from storing full hashes
    private final byte[] positionToHashes;
    private long hashCollisions;
    private double expectedHashCollisions;
    private JoinProcessor joinProcessor;
    private final JoinStatisticsCounter statisticsCounter;

    public HashBuildAndProbeTable(
            PagesHashStrategy pagesHashStrategy,
            int expectedPositions,
            PositionLinks.FactoryBuilder positionLinks,
            JoinProbe.JoinProbeFactory joinProbeFactory,
            List<Type> buildOutputTypes,
            Optional<JoinFilterFunction> filterFunction,
            LookupJoinOperators.JoinType joinType,
            boolean outputSingleMatch,
            boolean eagerCompact)
    {
        this.addresses = new LongArrayList(expectedPositions);
        this.pagesHashStrategy = requireNonNull(pagesHashStrategy, "pagesHashStrategy is null");
        this.channelCount = pagesHashStrategy.getChannelCount();
        this.positionCounts = new IntArrayList(1024);
        this.pageCount = 0;
        this.positionCount = 0;
        this.eagerCompact = eagerCompact;
        this.pagesMemorySize = 0;
        this.positionLinks = positionLinks;
        this.size = 0;
        this.hashCollisions = 0;
        this.expectedHashCollisions = 0;
        this.filterFunction = requireNonNull(filterFunction, "filterFunction cannot be null").orElse(null);
        this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
        this.joinProbeFactory = requireNonNull(joinProbeFactory, "joinProbeFactory is null");
        this.buildOutputTypes = buildOutputTypes;
        channels = (ObjectArrayList<Block>[]) new ObjectArrayList[types.size()];
        for (int i = 0; i < channels.length; i++) {
            channels[i] = ObjectArrayList.wrap(new Block[1024], 0);
        }
        // reserve memory for the arrays
        this.hashSize = HashCommon.arraySize(expectedPositions, 0.75f);
        mask = hashSize - 1;
        key = new int[hashSize];
        Arrays.fill(key, -1);
        positionToHashes = new byte[hashSize];
        this.statisticsCounter = new JoinStatisticsCounter(joinType);
        joinProcessor = new JoinProcessor(buildOutputTypes, joinProbeFactory,filterFunction,buildOutputTypes,joinType,outputSingleMatch,statisticsCounter);


    }
    public synchronized void addPage(Page page) {
        //Add page to channel and address
        if (page.getPositionCount() == 0) {
            return;
        }
        pageCount++;
        positionCount += page.getPositionCount();
        positionCounts.add(page.getPositionCount());
        int pageIndex = (channels.length > 0) ? channels[0].size() : 0;
        int startPosition = addresses.size();
        for (int i = 0; i < channels.length; i++) {
            Block block = page.getBlock(i);
            if (eagerCompact) {
                block = block.copyRegion(0, block.getPositionCount());
            }
            channels[i].add(block);
            pagesMemorySize += block.getRetainedSizeInBytes();
        }

        for (int position = 0; position < page.getPositionCount(); position++) {
            long sliceAddress = encodeSyntheticAddress(pageIndex, position);

            // this uses a long[] internally, so cap size to a nice round number for safety
            if (addresses.size() >= 2_000_000_000) {
                throw new TrinoException(GENERIC_INSUFFICIENT_RESOURCES, "Size of pages index cannot exceed 2 billion entries");
            }
            addresses.add(sliceAddress);
        }


        // We will process addresses in batches, to save memory on array of hashes.
        long hashCollisionsLocal = 0;

        for (int position = startPosition; position <= addresses.size(); position++) {
            long hash = readHashPosition(position);
            int pos = getHashPosition(hash, mask);

            // look for an empty slot or a slot containing this key
            while (key[pos] != -1) {
                int currentKey = key[pos];
                if (((byte) hash) == positionToHashes[currentKey] && positionEqualsPositionIgnoreNulls(currentKey, position)) {
                    // found a slot for this key
                    // link the new key position to the current key position
                    position = positionLinks.link(position, currentKey);

                    // key[pos] updated outside of this loop
                    break;
                }
                // increment position and mask to handler wrap around
                pos = (pos + 1) & mask;
                hashCollisionsLocal++;
            }

            key[pos] = position;
            }

            size = sizeOf(addresses.elements()) + pagesHashStrategy.getSizeInBytes() +
                    sizeOf(key) + sizeOf(positionToHashes);
            hashCollisions += hashCollisionsLocal;
            expectedHashCollisions = estimateNumberOfHashCollisions(addresses.size(), hashSize);

            estimatedSize = calculateEstimatedSize();
    }

    public synchronized Page joinPage(Page page) {
        return joinProcessor.joinPage(page);
    }
    @Override
    public final int getChannelCount()
    {
        return channelCount;
    }

    public int getPositionCount()
    {
        return addresses.size();
    }

    public long getInMemorySizeInBytes()
    {
        return INSTANCE_SIZE + size;
    }

    public long getHashCollisions()
    {
        return hashCollisions;
    }

    public double getExpectedHashCollisions()
    {
        return expectedHashCollisions;
    }

    public int getAddressIndex(int position, Page hashChannelsPage)
    {
        return getAddressIndex(position, hashChannelsPage, pagesHashStrategy.hashRow(position, hashChannelsPage));
    }

    public int getAddressIndex(int rightPosition, Page hashChannelsPage, long rawHash)
    {
        int pos = getHashPosition(rawHash, mask);

        while (key[pos] != -1) {
            if (positionEqualsCurrentRowIgnoreNulls(key[pos], (byte) rawHash, rightPosition, hashChannelsPage)) {
                return key[pos];
            }
            // increment position and mask to handler wrap around
            pos = (pos + 1) & mask;
        }
        return -1;
    }

    @Override
    public void appendTo(long position, PageBuilder pageBuilder, int outputChannelOffset)
    {
        long pageAddress = addresses.getLong(toIntExact(position));
        int blockIndex = decodeSliceIndex(pageAddress);
        int blockPosition = decodePosition(pageAddress);

        pagesHashStrategy.appendTo(blockIndex, blockPosition, pageBuilder, outputChannelOffset);
    }

    private boolean isPositionNull(int position)
    {
        long pageAddress = addresses.getLong(position);
        int blockIndex = decodeSliceIndex(pageAddress);
        int blockPosition = decodePosition(pageAddress);

        return pagesHashStrategy.isPositionNull(blockIndex, blockPosition);
    }

    private long readHashPosition(int position)
    {
        long pageAddress = addresses.getLong(position);
        int blockIndex = decodeSliceIndex(pageAddress);
        int blockPosition = decodePosition(pageAddress);

        return pagesHashStrategy.hashPosition(blockIndex, blockPosition);
    }

    private boolean positionEqualsCurrentRowIgnoreNulls(int leftPosition, byte rawHash, int rightPosition, Page rightPage)
    {
        if (positionToHashes[leftPosition] != rawHash) {
            return false;
        }

        long pageAddress = addresses.getLong(leftPosition);
        int blockIndex = decodeSliceIndex(pageAddress);
        int blockPosition = decodePosition(pageAddress);

        return pagesHashStrategy.positionEqualsRowIgnoreNulls(blockIndex, blockPosition, rightPosition, rightPage);
    }

    private boolean positionEqualsPositionIgnoreNulls(int leftPosition, int rightPosition)
    {
        long leftPageAddress = addresses.getLong(leftPosition);
        int leftBlockIndex = decodeSliceIndex(leftPageAddress);
        int leftBlockPosition = decodePosition(leftPageAddress);

        long rightPageAddress = addresses.getLong(rightPosition);
        int rightBlockIndex = decodeSliceIndex(rightPageAddress);
        int rightBlockPosition = decodePosition(rightPageAddress);

        return pagesHashStrategy.positionEqualsPositionIgnoreNulls(leftBlockIndex, leftBlockPosition, rightBlockIndex, rightBlockPosition);
    }

    private static int getHashPosition(long rawHash, long mask)
    {
        // Avalanches the bits of a long integer by applying the finalisation step of MurmurHash3.
        //
        // This function implements the finalisation step of Austin Appleby's <a href="http://sites.google.com/site/murmurhash/">MurmurHash3</a>.
        // Its purpose is to avalanche the bits of the argument to within 0.25% bias. It is used, among other things, to scramble quickly (but deeply) the hash
        // values returned by {@link Object#hashCode()}.
        //

        rawHash ^= rawHash >>> 33;
        rawHash *= 0xff51afd7ed558ccdL;
        rawHash ^= rawHash >>> 33;
        rawHash *= 0xc4ceb9fe1a85ec53L;
        rawHash ^= rawHash >>> 33;

        return (int) (rawHash & mask);
    }

    private long calculateEstimatedSize()
    {
        throw new java.lang.UnsupportedOperationException("Not implemented yet.");
    }

    @Override
    public final long getNextJoinPosition(long currentJoinPosition, int probePosition, Page allProbeChannelsPage)
    {
        if (positionLinks == null) {
            return -1;
        }
        return positionLinks.next(toIntExact(currentJoinPosition), probePosition, allProbeChannelsPage);
    }

    @Override
    public long getJoinPositionCount()
    {
        return addresses.size();
    }

    @Override
    public long joinPositionWithinPartition(long joinPosition)
    {
        return joinPosition;
    }

    @Override
    public long getJoinPosition(int position, Page hashChannelsPage, Page allChannelsPage)
    {
        int addressIndex = getAddressIndex(position, hashChannelsPage);
        return startJoinPosition(addressIndex, position, allChannelsPage);
    }

    @Override
    public long getJoinPosition(int position, Page hashChannelsPage, Page allChannelsPage, long rawHash)
    {
        int addressIndex = getAddressIndex(position, hashChannelsPage, rawHash);
        return startJoinPosition(addressIndex, position, allChannelsPage);
    }

    private long startJoinPosition(int currentJoinPosition, int probePosition, Page allProbeChannelsPage)
    {
        if (currentJoinPosition == -1) {
            return -1;
        }
        if (positionLinks == null) {
            return currentJoinPosition;
        }
        return positionLinks.start(currentJoinPosition, probePosition, allProbeChannelsPage);
    }

    @Override
    public boolean isJoinPositionEligible(long currentJoinPosition, int probePosition, Page allProbeChannelsPage)
    {
        return filterFunction == null || filterFunction.filter(toIntExact(currentJoinPosition), probePosition, allProbeChannelsPage);
    }

    @Override
    public boolean isEmpty()
    {
        return getJoinPositionCount() == 0;
    }

    @Override
    public void close()
    {
    }

}
