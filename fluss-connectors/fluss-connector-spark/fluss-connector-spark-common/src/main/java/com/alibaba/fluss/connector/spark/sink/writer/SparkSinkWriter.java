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

import com.alibaba.fluss.client.Connection;
import com.alibaba.fluss.client.ConnectionFactory;
import com.alibaba.fluss.client.table.Table;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.connector.spark.utils.SparkRowToFlussRowConverter;
import com.alibaba.fluss.metadata.TablePath;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.CompletableFuture;

/** Spark's {@link DataWriter} implementation for Fluss. */
public abstract class SparkSinkWriter implements DataWriter<InternalRow>, Closeable, Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(SparkSinkWriter.class);

    private static final long serialVersionUID = 1L;

    private transient Connection connection;
    protected transient Table table;
    protected final StructType sparkSchema;
    private transient SparkRowToFlussRowConverter dataConverter;
    private volatile Throwable asyncWriterException;

    public SparkSinkWriter(TablePath tablePath, StructType sparkSchema, Configuration flussConfig) {
        this.connection = ConnectionFactory.createConnection(flussConfig);
        this.table = connection.getTable(tablePath);
        this.sparkSchema = sparkSchema;
        this.dataConverter = createSparkRowToFlussRowConverter();
    }

    @Override
    public void write(InternalRow internalRow) throws IOException {
        checkAsyncException();
        CompletableFuture<Void> writeFuture = writeRow(dataConverter.toInternalRow(internalRow));
        writeFuture.exceptionally(
                exception -> {
                    if (asyncWriterException != null) {
                        asyncWriterException = exception;
                    }
                    return null;
                });
    }

    @Override
    public WriterCommitMessage commit() throws IOException {
        return null;
    }

    @Override
    public void abort() throws IOException {}

    @Override
    public void close() throws IOException {
        flush();
        // close table
        try {
            if (table != null) {
                table.close();
            }
        } catch (Exception e) {
            LOG.warn("Exception occurs while closing Fluss Table.", e);
        }
        table = null;

        // close connection
        try {
            if (connection != null) {
                connection.close();
            }
        } catch (Exception e) {
            LOG.warn("Exception occurs while closing Fluss Connection.", e);
        }
        connection = null;

        // close data converter
        try {
            if (dataConverter != null) {
                dataConverter.close();
            }
        } catch (Exception e) {
            LOG.warn("Exception occurs while closing Fluss RowData Converter.", e);
        }
        dataConverter = null;

        // Rethrow exception for the case in which close is called before writer() and flush().
        checkAsyncException();

        LOG.info("Finished closing Fluss sink function.");
    }

    abstract SparkRowToFlussRowConverter createSparkRowToFlussRowConverter();

    abstract CompletableFuture<Void> writeRow(com.alibaba.fluss.row.InternalRow internalRow);

    abstract void flush() throws IOException;

    protected void checkAsyncException() throws IOException {
        // reset this exception since we could close the writer later on
        Throwable throwable = asyncWriterException;
        if (throwable != null) {
            asyncWriterException = null;
            LOG.error("Exception occurs while write row to fluss.", throwable);
            throw new IOException(
                    "One or more Fluss Writer send requests have encountered exception", throwable);
        }
    }
}
