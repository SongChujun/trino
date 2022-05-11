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
package io.trino.server;

import io.trino.execution.StageId;
import io.trino.split.SplitSource;
import io.trino.sql.analyzer.FeaturesConfig;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkState;

public class DynamicJoinPushdownService
{
    private final Set<SplitSource> occupiedSources;

    private Map<SplitSource, StageId> occupationStageIdMap;

    private FeaturesConfig.ElasticJoinType elasticJoinType;

    public DynamicJoinPushdownService(FeaturesConfig.ElasticJoinType elasticJoinType)
    {
        this.occupiedSources = new HashSet<>();
        this.occupationStageIdMap = new HashMap<>();
        this.elasticJoinType = elasticJoinType;
    }

    public void registerOccupationStageId(SplitSource splitSource, StageId stageId)
    {
        checkState(!occupiedSources.contains(splitSource), "SplitSource is already occupied");
        this.occupiedSources.add(splitSource);
        this.occupationStageIdMap.put(splitSource, stageId);
    }

    public boolean inCompetition(SplitSource splitSource)
    {
        return !occupiedSources.contains(splitSource);
    }

    public boolean checkOccupied(SplitSource splitSource, StageId stageId)
    {
        return occupiedSources.contains(splitSource) && occupationStageIdMap.get(splitSource).equals(stageId);
    }

    public FeaturesConfig.ElasticJoinType getElasticJoinType()
    {
        return elasticJoinType;
    }
}
