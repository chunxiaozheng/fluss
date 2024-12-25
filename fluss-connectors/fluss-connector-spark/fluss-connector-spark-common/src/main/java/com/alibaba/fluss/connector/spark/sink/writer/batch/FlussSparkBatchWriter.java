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

package com.alibaba.fluss.connector.spark.sink.writer.batch;

import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.connector.spark.sink.writer.AppendSinkWriter;
import com.alibaba.fluss.connector.spark.sink.writer.UpsertSinkWriter;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TablePath;

import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;

/** Spark's {@link BatchWrite} implementation for Fluss. */
public class FlussSparkBatchWriter implements BatchWrite, Serializable {

    private static final long serialVersionUID = 1L;

    private final TablePath tablePath;
    private final StructType sparkSchema;
    private final TableDescriptor tableDescriptor;
    private final Configuration flussConfig;

    public FlussSparkBatchWriter(
            TablePath tablePath,
            StructType sparkSchema,
            TableDescriptor tableDescriptor,
            Configuration flussConfig) {
        this.sparkSchema = sparkSchema;
        this.tableDescriptor = tableDescriptor;
        this.tablePath = tablePath;
        this.flussConfig = flussConfig;
    }

    @Override
    public DataWriterFactory createBatchWriterFactory(PhysicalWriteInfo physicalWriteInfo) {
        return (partitionId, taskId) -> {
            if (tableDescriptor.hasPrimaryKey()) {
                return new UpsertSinkWriter(tablePath, sparkSchema, flussConfig);
            } else {
                return new AppendSinkWriter(tablePath, sparkSchema, flussConfig);
            }
        };
    }

    @Override
    public void commit(WriterCommitMessage[] writerCommitMessages) {}

    @Override
    public void abort(WriterCommitMessage[] writerCommitMessages) {}
}
