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
package io.trino.operator.join;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.trino.operator.PagesHashStrategy;
import io.trino.operator.SimplePagesHashStrategy;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.type.Type;
import io.trino.type.BlockTypeOperators;
import it.unimi.dsi.fastutil.HashCommon;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import org.openjdk.jol.info.ClassLayout;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.trino.operator.SyntheticAddress.decodePosition;
import static io.trino.operator.SyntheticAddress.decodeSliceIndex;
import static io.trino.operator.SyntheticAddress.encodeSyntheticAddress;
import static io.trino.operator.join.LookupJoinOperatorFactory.JoinType.FULL_OUTER;
import static io.trino.operator.join.LookupJoinOperatorFactory.JoinType.PROBE_OUTER;
import static io.trino.spi.StandardErrorCode.GENERIC_INSUFFICIENT_RESOURCES;
import static io.trino.util.HashCollisionsEstimator.estimateNumberOfHashCollisions;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

// This implementation assumes arrays used in the hash are always a power of 2
public final class HashBuildAndProbeTable
        implements LookupSource
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(HashBuildAndProbeTable.class).instanceSize();
    //    private static final DataSize CACHE_SIZE = DataSize.of(128, KILOBYTE);
    private final List<List<Block>> channels;
    private final IntArrayList positionCounts;
    private final boolean eagerCompact;
    @Nullable
    private final JoinFilterFunction filterFunction;
    private final JoinProbe.JoinProbeFactory joinProbeFactory;
    private final LongArrayList addresses;
    private final PagesHashStrategy pagesHashStrategy;
    private final int channelCount;
    private final int mask;
    private final int[] key;
    private final int hashSize;
    // Native array of hashes for faster collisions resolution compared
    // to accessing values in blocks. We use bytes to reduce memory foot print
    // and there is no performance gain from storing full hashes
    private byte[] positionToHashes;
    private final JoinStatisticsCounter statisticsCounter;
    private final SettableFuture<Boolean> hashBuildFinishedFuture;
    private int pageCount;
    private int positionCount;
    private long pagesMemorySize;
    private long estimatedSize;
    private final List<Type> types;
    private AdaptiveJoinPositionLinks positionLinks;
    private final List<Type> buildOutputTypes;
    private long size;
    private long hashCollisions;
    private double expectedHashCollisions;
    private final JoinProcessor joinProcessor;
    private long joinPageEntryCnt;
    private long buildPageEntryCnt;

    public HashBuildAndProbeTable(
            List<Type> types, //from buildSource.getTypes() -> ; from layout
            OptionalInt hashChannel,
            List<Integer> joinChannels,
            List<Type> buildOutputTypes, // from buildOutput channels
            Optional<List<Integer>> buildOutputChannels,
            BlockTypeOperators blockTypeOperators,
            int expectedPositions,
            JoinProbe.JoinProbeFactory joinProbeFactory,
            LookupJoinOperatorFactory.JoinType joinType,
            boolean outputSingleMatch,
            boolean eagerCompact)
    {
        this.positionLinks = new AdaptiveJoinPositionLinks(expectedPositions);

        this.addresses = new LongArrayList(expectedPositions);
        this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));

        channels = new ArrayList<>();
        for (int i = 0; i < types.size(); i++) {
            channels.add(new ArrayList<>());
        }
        this.pagesHashStrategy = new SimplePagesHashStrategy(
                types,
                buildOutputChannels.orElse(rangeList(buildOutputTypes.size())),
                channels,
                joinChannels,
                hashChannel,
                Optional.empty(),
                blockTypeOperators);
        this.channelCount = pagesHashStrategy.getChannelCount();
        this.positionCount = 0;
        this.positionCounts = new IntArrayList(1024);
        this.pageCount = 0;
        this.eagerCompact = eagerCompact;
        this.pagesMemorySize = 0;
        this.size = 0;
        this.hashCollisions = 0;
        this.expectedHashCollisions = 0;

        this.filterFunction = null; //!hacky
        this.joinProbeFactory = requireNonNull(joinProbeFactory, "joinProbeFactory is null");
        this.buildOutputTypes = buildOutputTypes;

        // reserve memory for the arrays
        this.hashSize = HashCommon.arraySize(expectedPositions, 0.75f);
        mask = hashSize - 1;
        key = new int[hashSize];
        Arrays.fill(key, -1);
        positionToHashes = new byte[hashSize];
        this.statisticsCounter = new JoinStatisticsCounter(joinType);
        this.hashBuildFinishedFuture = SettableFuture.create();
        joinProcessor = new JoinProcessor(buildOutputTypes, joinProbeFactory, joinType, outputSingleMatch, statisticsCounter);
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

    public ListenableFuture<Boolean> getBuildFinishedFuture()
    {
        return hashBuildFinishedFuture;
    }

    private List<Integer> rangeList(int endExclusive)
    {
        return IntStream.range(0, endExclusive)
                .boxed()
                .collect(toImmutableList());
    }

    public void addPage(Page page)
    {
        //Add page to channel and address
        if (page.getPositionCount() == 0) {
            return;
        }
        pageCount++;
        positionCount += page.getPositionCount();
        positionCounts.add(page.getPositionCount());
        int pageIndex = (channels.size() > 0) ? channels.get(0).size() : 0;
        int startPosition = addresses.size();
        for (int i = 0; i < channels.size(); i++) {
            Block block = page.getBlock(i);
            if (eagerCompact) {
                block = block.copyRegion(0, block.getPositionCount());
            }
            channels.get(i).add(block);
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

        for (int position = startPosition; position < addresses.size(); position++) {
            long hash = readHashPosition(position);
            positionToHashes[position] = (byte) hash;
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

    public List<Page> joinPage(Page page)
    {
        List<Page> res = joinProcessor.join(page);
        this.joinPageEntryCnt += 0;
        return res;
    }

    //only clears the data, not the controlling information
    public void reset()
    {
        for (List<Block> blockList : channels) {
            blockList.clear();
        }
        this.positionLinks.reset();
        this.addresses.clear();
        this.positionCount = 0;
        this.positionCounts.clear();
        this.pageCount = 0;
        this.pagesMemorySize = 0;
        this.size = 0;
        this.hashCollisions = 0;
        this.expectedHashCollisions = 0;
        Arrays.fill(positionToHashes, (byte) 0);
    }

    public void setBuildFinished()
    {
        hashBuildFinishedFuture.set(true);
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

    private long calculateEstimatedSize()
    {
        return 0;
    }

    @Override
    public final long getNextJoinPosition(long currentJoinPosition, int probePosition, Page allProbeChannelsPage)
    {
        if (positionLinks == null) {
            return -1;
        }
        return positionLinks.next(toIntExact(currentJoinPosition));
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
        return startJoinPosition(addressIndex);
    }

    @Override
    public long getJoinPosition(int position, Page hashChannelsPage, Page allChannelsPage, long rawHash)
    {
        int addressIndex = getAddressIndex(position, hashChannelsPage, rawHash);
        return startJoinPosition(addressIndex);
    }

    private long startJoinPosition(int currentJoinPosition)
    {
        if (currentJoinPosition == -1) {
            return -1;
        }
        if (positionLinks == null) {
            return currentJoinPosition;
        }
        return positionLinks.start(currentJoinPosition);
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
        for (List<Block> channel : channels) {
            channel.clear();
        }
        channels.clear();
        addresses.clear();
        addresses.trim();
        positionCounts.clear();
        positionCounts.trim();
        positionToHashes = null;
        positionLinks = null;
        positionCount = 0;
    }

    private class JoinProcessor
    {
        private final JoinProbe.JoinProbeFactory joinProbeFactory;
        private final boolean probeOnOuterSide;
        private final JoinStatisticsCounter statisticsCounter;
        private final List<Type> buildOutputTypes;
        private final LookupJoinPageBuilder pageBuilder;
        private final IntArrayList probeIndexBuilder;
        private boolean currentProbePositionProducedRow;
        private boolean isSequentialProbeIndices;
        private int estimatedProbeBlockBytes;
        private long joinPosition = -1;
        private int joinSourcePositions;
        private final boolean outputSingleMatch;
        private JoinProbe probe;

        private JoinProcessor(List<Type> buildOutputTypes,
                JoinProbe.JoinProbeFactory joinProbeFactory,
                LookupJoinOperatorFactory.JoinType joinType,
                boolean outputSingleMatch,
                JoinStatisticsCounter statisticsCounter)
        {
            this.buildOutputTypes = buildOutputTypes;
            this.joinProbeFactory = joinProbeFactory;
            this.pageBuilder = new LookupJoinPageBuilder(buildOutputTypes);
            this.probeIndexBuilder = new IntArrayList();
            this.currentProbePositionProducedRow = false;
            this.isSequentialProbeIndices = false;
            this.estimatedProbeBlockBytes = 0;
            this.outputSingleMatch = outputSingleMatch;
            this.statisticsCounter = statisticsCounter;

            probeOnOuterSide = joinType == PROBE_OUTER || joinType == FULL_OUTER;
        }

        private Page joinPage()
        {
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
            }
            while (true);
            if (!pageBuilder.isEmpty()) {
                Page outputPage = pageBuilder.build(probe);
                pageBuilder.reset();
                return outputPage;
            }
            return null;
        }

        public List<Page> join(Page page)
        {
            ImmutableList.Builder<Page> res = new ImmutableList.Builder<>();
            probe = joinProbeFactory.createJoinProbe(page);
            while (!probe.isFinished()) {
                Page joinResult = joinPage();
                if (joinResult != null) {
                    res.add(joinResult);
                }
            }
            return res.build();
        }

        public void reset()
        {
//            hashBuildFinishedFuture.set(true);
            this.joinPosition = -1;
            this.pageBuilder.reset();
            this.currentProbePositionProducedRow = false;
            this.isSequentialProbeIndices = false;
            this.estimatedProbeBlockBytes = 0;
            this.probeIndexBuilder.clear();
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
}
