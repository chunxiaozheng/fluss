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

package com.alibaba.fluss.connector.spark.sink.writer;

import com.alibaba.fluss.client.table.writer.UpsertWriter;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.connector.spark.utils.SparkRowToFlussRowConverter;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.types.DataType;

import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

/** A upsert sink writer for fluss primary key table. */
public class UpsertSinkWriter extends SparkSinkWriter {

    private static final long serialVersionUID = 1L;

    private transient UpsertWriter upsertWriter;

    public UpsertSinkWriter(
            TablePath tablePath, StructType sparkSchema, Configuration flussConfig) {
        super(tablePath, sparkSchema, flussConfig);
        this.upsertWriter = table.getUpsertWriter();
    }

    @Override
    CompletableFuture<Void> writeRow(InternalRow internalRow) {
        return upsertWriter.upsert(internalRow);
    }

    @Override
    SparkRowToFlussRowConverter createSparkRowToFlussRowConverter() {
        DataType[] flussDataTypes =
                table.getDescriptor().getSchema().getColumns().stream()
                        .map(column -> column.getDataType())
                        .toArray(DataType[]::new);
        return SparkRowToFlussRowConverter.create(
                sparkSchema, flussDataTypes, table.getDescriptor().getKvFormat());
    }

    @Override
    void flush() throws IOException {
        upsertWriter.flush();
        checkAsyncException();
    }
}
