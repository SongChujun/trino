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

import com.google.common.util.concurrent.SettableFuture;
import io.trino.operator.PagesIndex;
import io.trino.spi.type.Type;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class SortMergeJoinBridge
{
    public List<PagesIndex> leftPagesIndexList;
    public List<PagesIndex> rightPagesIndexList;
    public List<SettableFuture<Boolean>> sortFinishedFutureList;
    public List<AtomicInteger> sortFinishedCntList;

    private int sortedPagesIdx;
    private int sortedFutureIdx;

    public SortMergeJoinBridge(int size, List<Type> leftSourceTypes, List<Type> rightSourceTypes, PagesIndex.Factory pagesIndexFactory, int expectedPositions)
    {
        leftPagesIndexList = new ArrayList<>();
        sortFinishedFutureList = new ArrayList<>();
        rightPagesIndexList = new ArrayList<>();
        sortFinishedCntList = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            leftPagesIndexList.add(pagesIndexFactory.newPagesIndex(leftSourceTypes, expectedPositions));
            rightPagesIndexList.add(pagesIndexFactory.newPagesIndex(rightSourceTypes, expectedPositions));
            sortFinishedFutureList.add(SettableFuture.create());
            sortFinishedCntList.add(new AtomicInteger(0));
        }
        sortedPagesIdx = 0;
        sortedFutureIdx = 0;
    }

    public PagesIndex getLeftPagesIndex(int pos)
    {
        return leftPagesIndexList.get(pos);
    }

    public PagesIndex getRightPagesIndex(int pos)
    {
        return rightPagesIndexList.get(pos);
    }

    public SettableFuture<Boolean> getSortFinishedFuture(int pos)
    {
        return sortFinishedFutureList.get(pos);
    }

    public AtomicInteger getSortFinishedCnt(int pos)
    {
        return sortFinishedCntList.get(pos);
    }

    public List<PagesIndex> getNextSortedPagesPair()
    {
        List<PagesIndex> res = new ArrayList<>();
        res.add(leftPagesIndexList.get(sortedPagesIdx));
        res.add(rightPagesIndexList.get(sortedPagesIdx));
        sortedPagesIdx += 1;
        return res;
    }

    public SettableFuture<Boolean> getNextFinishedFuture()
    {
        SettableFuture<Boolean> finishedFuture = sortFinishedFutureList.get(sortedFutureIdx);
        sortedFutureIdx += 1;
        return finishedFuture;
    }
}
