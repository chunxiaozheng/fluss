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

package com.alibaba.fluss.connector.spark.source;

import com.alibaba.fluss.metadata.TableBucket;

import org.apache.spark.sql.connector.read.InputPartition;

import javax.annotation.Nullable;

/** Spark's {@link InputPartition} implementation for Fluss. */
public class FlussInputPartition implements InputPartition {

    private final TableBucket tableBucket;

    public FlussInputPartition(Long tableId, @Nullable Long partitionId, Integer bucketId) {
        this.tableBucket = new TableBucket(tableId, partitionId, bucketId);
    }

    public TableBucket getTableBucket() {
        return tableBucket;
    }
}
