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

import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.connector.spark.source.reader.batch.FlussSparkBatch;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TablePath;

import org.apache.spark.sql.connector.metric.CustomMetric;
import org.apache.spark.sql.connector.metric.CustomTaskMetric;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.streaming.ContinuousStream;
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructType;

import javax.annotation.Nullable;

/** Spark's {@link Scan} implementation for Fluss. */
public class FlussScan implements Scan {

    private final TablePath tablePath;
    private final StructType sparkSchema;
    private final TableDescriptor tableDescriptor;
    private final Configuration flussConfig;

    private final int limit;
    private final Filter[] filters;
    @Nullable private final int[] projectedFields;

    public FlussScan(
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
    public String description() {
        return "Fluss scan.";
    }

    @Override
    public Batch toBatch() {
        return new FlussSparkBatch(
                tablePath,
                sparkSchema,
                tableDescriptor,
                flussConfig,
                limit,
                filters,
                projectedFields);
    }

    @Override
    public MicroBatchStream toMicroBatchStream(String checkpointLocation) {
        return Scan.super.toMicroBatchStream(checkpointLocation);
    }

    @Override
    public ContinuousStream toContinuousStream(String checkpointLocation) {
        return Scan.super.toContinuousStream(checkpointLocation);
    }

    @Override
    public CustomMetric[] supportedCustomMetrics() {
        return Scan.super.supportedCustomMetrics();
    }

    @Override
    public CustomTaskMetric[] reportDriverMetrics() {
        return Scan.super.reportDriverMetrics();
    }

    @Override
    public ColumnarSupportMode columnarSupportMode() {
        return Scan.super.columnarSupportMode();
    }

    @Override
    public StructType readSchema() {
        return sparkSchema;
    }
}
