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

package com.alibaba.fluss.connector.spark.source.reader.batch;

import com.alibaba.fluss.client.Connection;
import com.alibaba.fluss.client.ConnectionFactory;
import com.alibaba.fluss.client.admin.Admin;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.connector.spark.source.FlussInputPartition;
import com.alibaba.fluss.exception.FlussRuntimeException;
import com.alibaba.fluss.metadata.PartitionInfo;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TablePath;

import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructType;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/** Spark's {@link Batch} implementation for Fluss. */
public class FlussSparkBatch implements Batch, Serializable {

    private static final long serialVersionUID = 1L;

    private final TablePath tablePath;
    private final StructType sparkSchema;
    private final TableDescriptor tableDescriptor;
    private final Configuration flussConfig;

    private final int limit;
    private final Filter[] filters;
    @Nullable private final int[] projectedFields;

    public FlussSparkBatch(
            TablePath tablePath,
            StructType sparkSchema,
            TableDescriptor tableDescriptor,
            Configuration flussConfig,
            int limit,
            Filter[] filters,
            @Nullable int[] projectedFields) {
        this.tablePath = tablePath;
        this.sparkSchema = sparkSchema;
        this.tableDescriptor = tableDescriptor;
        this.flussConfig = flussConfig;
        this.limit = limit;
        this.filters = filters;
        this.projectedFields = projectedFields;
    }

    @Override
    public InputPartition[] planInputPartitions() {
        if (isLookup()) {
            // the FlussInputPartition is no use in lookup mode
            return new InputPartition[] {new FlussInputPartition(0L, null, 0)};
        }
        // generate input partitions by tableId, partitionId and bucketId
        return generateInputPartitions();
    }

    @Override
    public PartitionReaderFactory createReaderFactory() {
        return partition -> {
            if (isLookup()) {
                return new LookupBatchReader(tablePath, sparkSchema, flussConfig, filters);
            } else if (isLimitScan()) {
                return new LimitScanBatchReader(
                        tablePath,
                        sparkSchema,
                        flussConfig,
                        (FlussInputPartition) partition,
                        limit,
                        projectedFields);
            }
            throw new UnsupportedOperationException(
                    String.format(
                            "fluss primary key table support lookup or limit scan, "
                                    + "log table only support limit scan in batch mode now, %s is a %s",
                            tablePath.toString(),
                            tableDescriptor.hasPrimaryKey() ? "primary key table" : "log table"));
        };
    }

    private boolean isLookup() {
        return tableDescriptor.hasPrimaryKey()
                && filters != null
                && filters.length > 0
                && filters.length == tableDescriptor.getSchema().getPrimaryKeyIndexes().length;
    }

    private boolean isLimitScan() {
        return limit > 0;
    }

    private InputPartition[] generateInputPartitions() {
        int bucketCount =
                tableDescriptor
                        .getTableDistribution()
                        .orElseThrow(
                                () -> new IllegalStateException("Table distribution is not set."))
                        .getBucketCount()
                        .orElseThrow(() -> new IllegalStateException("Bucket count is not set."));

        try (Connection connection = ConnectionFactory.createConnection(flussConfig);
                Admin flussAdmin = connection.getAdmin()) {
            long tableId = flussAdmin.getTable(tablePath).get().getTableId();
            List<FlussInputPartition> inputPartitions;
            if (tableDescriptor.isPartitioned()) {
                List<PartitionInfo> partitionInfos = flussAdmin.listPartitionInfos(tablePath).get();
                inputPartitions =
                        partitionInfos.stream()
                                .flatMap(
                                        partitionInfo ->
                                                IntStream.range(0, bucketCount)
                                                        .mapToObj(
                                                                bucketId ->
                                                                        new FlussInputPartition(
                                                                                tableId,
                                                                                partitionInfo
                                                                                        .getPartitionId(),
                                                                                bucketId)))
                                .collect(Collectors.toList());
            } else {
                inputPartitions =
                        IntStream.range(0, bucketCount)
                                .mapToObj(
                                        bucketId ->
                                                new FlussInputPartition(tableId, null, bucketId))
                                .collect(Collectors.toList());
            }
            return inputPartitions.toArray(new FlussInputPartition[0]);
        } catch (Exception e) {
            throw new FlussRuntimeException(e);
        }
    }
}
