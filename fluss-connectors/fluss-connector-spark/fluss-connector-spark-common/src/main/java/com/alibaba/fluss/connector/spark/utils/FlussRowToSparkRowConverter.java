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

import com.alibaba.fluss.client.scanner.ScanRecord;
import com.alibaba.fluss.row.BinaryString;
import com.alibaba.fluss.row.Decimal;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.row.TimestampLtz;
import com.alibaba.fluss.row.TimestampNtz;
import com.alibaba.fluss.types.DataType;
import com.alibaba.fluss.utils.Preconditions;

import org.apache.spark.sql.catalyst.expressions.SpecificInternalRow;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;

import java.io.Serializable;
import java.util.List;

/**
 * A converter to convert Fluss's {@link InternalRow} to Spark's {@link
 * org.apache.spark.sql.catalyst.InternalRow}.
 */
public class FlussRowToSparkRowConverter {

    private final int fieldLength;
    private final StructType sparkSchema;
    private final InternalRow.FieldGetter[] flussFieldGetters;
    private final FlussDeserializationConverter[] toSparkFieldConverters;

    /**
     * convert constructor.
     *
     * @param sparkSchema the spark schema of the target, which only contains the selected fields
     * @param flussDataTypes the fluss data types which correspond with the sparkSchema
     */
    public FlussRowToSparkRowConverter(StructType sparkSchema, List<DataType> flussDataTypes) {
        Preconditions.checkArgument(
                sparkSchema.fields().length == flussDataTypes.size(),
                String.format(
                        "the fields in sparkSchema do not correspond to those in fluss data types, "
                                + "field length in sparkSchema is %d, while fluss data types length is %s",
                        sparkSchema.fields().length, flussDataTypes.size()));
        this.fieldLength = sparkSchema.fields().length;
        this.sparkSchema = sparkSchema;
        this.flussFieldGetters = new InternalRow.FieldGetter[fieldLength];
        this.toSparkFieldConverters = new FlussDeserializationConverter[fieldLength];
        for (int i = 0; i < fieldLength; i++) {
            flussFieldGetters[i] = InternalRow.createFieldGetter(flussDataTypes.get(i), i);
            toSparkFieldConverters[i] = createNullableInternalConverter(flussDataTypes.get(i));
        }
    }

    public org.apache.spark.sql.catalyst.InternalRow toInternalRow(ScanRecord scanRecord) {
        return toInternalRow(scanRecord.getRow());
    }

    public org.apache.spark.sql.catalyst.InternalRow toInternalRow(InternalRow rowData) {
        org.apache.spark.sql.catalyst.InternalRow internalRow =
                new SpecificInternalRow(sparkSchema);
        for (int i = 0; i < fieldLength; i++) {
            internalRow.update(
                    i,
                    toSparkFieldConverters[i].deserialize(
                            flussFieldGetters[i].getFieldOrNull(rowData)));
        }
        return internalRow;
    }

    /**
     * Create a nullable runtime {@link FlussDeserializationConverter} from given {@link DataType}.
     */
    protected FlussDeserializationConverter createNullableInternalConverter(
            DataType flussDataType) {
        return wrapIntoNullableInternalConverter(createInternalConverter(flussDataType));
    }

    protected FlussDeserializationConverter wrapIntoNullableInternalConverter(
            FlussDeserializationConverter flussDeserializationConverter) {
        return val -> {
            if (val == null) {
                return null;
            } else {
                return flussDeserializationConverter.deserialize(val);
            }
        };
    }

    /**
     * Runtime converter to convert field in Fluss's {@link InternalRow} to Spark's {@link
     * org.apache.spark.sql.catalyst.InternalRow} type object.
     */
    @FunctionalInterface
    public interface FlussDeserializationConverter extends Serializable {

        /**
         * Convert a Fluss field object of {@link InternalRow} to the Spark's internal data
         * structure object.
         *
         * @param flussField A single field of a {@link InternalRow}
         */
        Object deserialize(Object flussField);
    }

    private FlussDeserializationConverter createInternalConverter(DataType flussDataType) {
        switch (flussDataType.getTypeRoot()) {
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
                return val -> UTF8String.fromBytes(((BinaryString) val).toBytes());
            case DECIMAL:
                return val -> {
                    Decimal decimal = (Decimal) val;
                    return org.apache.spark.sql.types.Decimal.apply(
                            decimal.toBigDecimal(), decimal.precision(), decimal.scale());
                };
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return val -> {
                    TimestampNtz timestampNtz = (TimestampNtz) val;
                    long millis = timestampNtz.getMillisecond();
                    long nanoOfMillis = timestampNtz.getNanoOfMillisecond();
                    return millis * 1_000 + nanoOfMillis / 1_000;
                };
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return val -> {
                    TimestampLtz timestampLtz = (TimestampLtz) val;
                    return timestampLtz.toEpochMicros();
                };
            default:
                throw new UnsupportedOperationException("Unsupported data type: " + flussDataType);
        }
    }
}
