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
import io.trino.operator.PagesIndex;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.type.Type;

import java.util.List;

import static io.trino.operator.SyntheticAddress.decodePosition;
import static io.trino.operator.SyntheticAddress.decodeSliceIndex;

public class SortMergePageBuilder
{
    private final PagesIndex leftPagesIndex;
    private final PagesIndex rightPagesIndex;
    private final List<Type> leftOutputTypes;
    private final List<Type> rightOutputTypes;
    private final List<Integer> leftOutputChannels;
    private final List<Integer> rightOutputChannels;
    private final PageBuilder pageBuilder;

    public SortMergePageBuilder(PagesIndex leftPagesIndex, PagesIndex rightPagesIndex, List<Type> leftOutputTypes, List<Type> rightOutputTypes, List<Integer> leftOutputChannels, List<Integer> rightOutputChannels)
    {
        this.leftPagesIndex = leftPagesIndex;
        this.rightPagesIndex = rightPagesIndex;
        this.leftOutputTypes = leftOutputTypes;
        this.rightOutputTypes = rightOutputTypes;
        this.leftOutputChannels = leftOutputChannels;
        this.rightOutputChannels = rightOutputChannels;
        List<Type> outputTypes = ImmutableList.<Type>builder().addAll(leftOutputTypes).addAll(rightOutputTypes).build();
        this.pageBuilder = new PageBuilder(outputTypes);
    }

    public boolean isFull()
    {
        return pageBuilder.isFull();
    }

    public void appendRow(int leftPos, int rightPos, int outputChannelOffset)
    {
        int leftPageIndex = decodeSliceIndex(leftPos);
        int leftPagePosition = decodePosition(leftPos);
        int rightPageIndex = decodeSliceIndex(rightPos);
        int rightPagePosition = decodePosition(rightPos);

        int i = 0;
        for (int leftOutputIndex : leftOutputChannels) {
            Type type = leftOutputTypes.get(i++);
            Block block = leftPagesIndex.getChannel(leftOutputIndex).get(leftPageIndex);
            type.appendTo(block, leftPagePosition, pageBuilder.getBlockBuilder(outputChannelOffset));
            outputChannelOffset++;
        }

        i = 0;
        for (int rightOutputIndex : rightOutputChannels) {
            Type type = rightOutputTypes.get(i++);
            Block block = rightPagesIndex.getChannel(rightOutputIndex).get(rightPageIndex);
            type.appendTo(block, rightPagePosition, pageBuilder.getBlockBuilder(outputChannelOffset));
            outputChannelOffset++;
        }
    }

    public void reset()
    {
        pageBuilder.reset();
    }

    public Page buildPage()
    {
        return pageBuilder.build();
    }
}
