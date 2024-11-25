/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.connector.flink.source.enumerator.initializer;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.Map;

/**
 * An implementation of {@link OffsetsInitializer} which initializes the offsets.
 *
 * <p>If the user don't specify the offsets for a bucket, the initial offset of the bucket will be
 * the earliest offset.
 *
 * <p>Package private and should be instantiated via {@link OffsetsInitializer}.
 */
public class SnapshotOffsetsInitializer implements OffsetsInitializer {
    private static final long serialVersionUID = 1649702397250402877L;

    /**
     * For table with primary key. This method will be invoked only when the kv snapshot not exists.
     */
    @Override
    public Map<Integer, Long> getBucketOffsets(
            @Nullable String partitionName,
            Collection<Integer> buckets,
            BucketOffsetsRetriever bucketOffsetsRetriever) {
        return bucketOffsetsRetriever.earliestOffsets(partitionName, buckets);
    }
}