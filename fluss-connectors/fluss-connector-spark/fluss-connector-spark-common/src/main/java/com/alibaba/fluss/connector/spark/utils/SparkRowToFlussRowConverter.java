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

package com.alibaba.fluss.connector.spark.utils;

import com.alibaba.fluss.metadata.KvFormat;
import com.alibaba.fluss.row.BinaryString;
import com.alibaba.fluss.row.Decimal;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.row.TimestampLtz;
import com.alibaba.fluss.row.TimestampNtz;
import com.alibaba.fluss.row.encode.IndexedRowEncoder;
import com.alibaba.fluss.row.encode.RowEncoder;
import com.alibaba.fluss.row.indexed.IndexedRow;
import com.alibaba.fluss.types.DataType;
import com.alibaba.fluss.types.DecimalType;

import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;

import java.io.Serializable;

/**
 * A converter to convert Spark's {@link org.apache.spark.sql.catalyst.InternalRow} to Fluss's
 * {@link InternalRow}.
 */
public class SparkRowToFlussRowConverter implements AutoCloseable {

    private final StructType sparkSchema;
    private final DataType[] flussDataTypes;
    private final FlussSerializationConverter[] toFlussFieldConverters;
    private final RowEncoder rowEncoder;

    /** Create a {@link SparkRowToFlussRowConverter} which will convert to {@link IndexedRow}. */
    public static SparkRowToFlussRowConverter create(
            StructType sparkSchema, DataType[] flussDataTypes) {
        RowEncoder rowEncoder = new IndexedRowEncoder(flussDataTypes);
        return new SparkRowToFlussRowConverter(sparkSchema, flussDataTypes, rowEncoder);
    }

    /** Create a {@link SparkRowToFlussRowConverter} according to the given {@link KvFormat}. */
    public static SparkRowToFlussRowConverter create(
            StructType sparkSchema, DataType[] flussDataTypes, KvFormat kvFormat) {
        RowEncoder rowEncoder = RowEncoder.create(kvFormat, flussDataTypes);
        return new SparkRowToFlussRowConverter(sparkSchema, flussDataTypes, rowEncoder);
    }

    private SparkRowToFlussRowConverter(
            StructType sparkSchema, DataType[] flussDataTypes, RowEncoder rowEncoder) {
        this.sparkSchema = sparkSchema;
        this.flussDataTypes = flussDataTypes;

        // create field converter to convert field from flink to fluss.
        this.toFlussFieldConverters = new FlussSerializationConverter[flussDataTypes.length];
        for (int i = 0; i < flussDataTypes.length; i++) {
            toFlussFieldConverters[i] = createNullableInternalConverter(flussDataTypes[i]);
        }

        this.rowEncoder = rowEncoder;
    }

    public InternalRow toInternalRow(org.apache.spark.sql.catalyst.InternalRow rowData) {
        rowEncoder.startNewRow();
        for (int i = 0; i < flussDataTypes.length; i++) {
            rowEncoder.encodeField(
                    i,
                    toFlussFieldConverters[i].serialize(
                            rowData.get(i, sparkSchema.fields()[i].dataType())));
        }
        return rowEncoder.finishRow();
    }

    @Override
    public void close() throws Exception {
        if (rowEncoder != null) {
            rowEncoder.close();
        }
    }

    private FlussSerializationConverter createNullableInternalConverter(DataType flussType) {
        return wrapIntoNullableInternalConverter(createInternalConverter(flussType));
    }

    private FlussSerializationConverter wrapIntoNullableInternalConverter(
            FlussSerializationConverter flussSerializationConverter) {
        return val -> {
            if (val == null) {
                return null;
            } else {
                return flussSerializationConverter.serialize(val);
            }
        };
    }

    /**
     * Runtime converter to convert field in Spark's {@link
     * org.apache.spark.sql.catalyst.InternalRow} to Fluss's {@link InternalRow} type object.
     */
    @FunctionalInterface
    public interface FlussSerializationConverter extends Serializable {
        /**
         * Convert a Spark field object of {@link org.apache.spark.sql.catalyst.InternalRow} to the
         * Fluss's internal data structure object.
         *
         * @param sparkField A single field of a {@link org.apache.spark.sql.catalyst.InternalRow}
         */
        Object serialize(Object sparkField);
    }

    private FlussSerializationConverter createInternalConverter(DataType flussType) {
        switch (flussType.getTypeRoot()) {
            case BOOLEAN:
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            case BYTES:
            case BINARY:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                return val -> val;
            case CHAR:
            case STRING:
                return val -> {
                    UTF8String utf8String = (UTF8String) val;
                    return BinaryString.fromString(utf8String.toString());
                };
            case DECIMAL:
                return val -> {
                    org.apache.spark.sql.types.Decimal decimal =
                            (org.apache.spark.sql.types.Decimal) val;
                    DecimalType decimalType = (DecimalType) flussType;
                    return Decimal.fromBigDecimal(
                            decimal.toJavaBigDecimal(),
                            decimalType.getPrecision(),
                            decimalType.getScale());
                };
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return val -> {
                    long micros = (long) val;
                    return TimestampLtz.fromEpochMicros(micros);
                };
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return val -> {
                    long micros = (long) val;
                    long millis = micros / 1_000;
                    int nanoOfMillis = (int) ((micros % 1_000) * 1_000);
                    return TimestampNtz.fromMillis(millis, nanoOfMillis);
                };
            default:
                // TODO: support more data types.
                throw new UnsupportedOperationException(
                        "Fluss Unsupported data type: " + flussType);
        }
    }
}
