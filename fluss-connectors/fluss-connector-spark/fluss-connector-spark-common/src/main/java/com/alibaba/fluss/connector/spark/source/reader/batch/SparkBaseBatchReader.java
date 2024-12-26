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
import com.alibaba.fluss.client.table.Table;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.connector.spark.utils.FlussRowToSparkRowConverter;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.types.DataType;
import com.alibaba.fluss.types.RowType;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;

/** Spark's {@link PartitionReader} implementation for Fluss. */
public abstract class SparkBaseBatchReader implements PartitionReader<InternalRow> {

    private static final Logger LOG = LoggerFactory.getLogger(SparkBaseBatchReader.class);

    @Nullable protected final int[] projectedFields;
    protected final TablePath tablePath;
    protected final FlussRowToSparkRowConverter dataConverter;

    protected Connection connection;
    protected Table table;
    protected InternalRow currentRow;

    public SparkBaseBatchReader(
            TablePath tablePath,
            StructType sparkSchema,
            Configuration flussConfig,
            @Nullable int[] projectedFields) {
        this.tablePath = tablePath;
        this.connection = ConnectionFactory.createConnection(flussConfig);
        this.table = connection.getTable(tablePath);
        this.projectedFields = projectedFields;
        this.dataConverter = new FlussRowToSparkRowConverter(sparkSchema, getProjectDataTypes());
    }

    @Override
    public InternalRow get() {
        return currentRow;
    }

    @Override
    public void close() {
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
    }

    private List<DataType> getProjectDataTypes() {
        RowType rowType = table.getDescriptor().getSchema().toRowType();
        if (projectedFields == null) {
            return rowType.getChildren();
        }
        List<DataType> dataTypes = new ArrayList<>(projectedFields.length);
        for (int i = 0; i < projectedFields.length; i++) {
            dataTypes.add(rowType.getTypeAt(projectedFields[i]));
        }
        return dataTypes;
    }
}
