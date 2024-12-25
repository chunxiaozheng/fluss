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
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.row.TimestampLtz;
import com.alibaba.fluss.row.TimestampNtz;
import com.alibaba.fluss.types.DataType;

import org.apache.spark.sql.catalyst.expressions.SpecificInternalRow;
import org.apache.spark.sql.types.CharType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link com.alibaba.fluss.connector.spark.utils.SparkRowToFlussRowConverter}. */
public class SparkRowToFlussRowConverterTest {

    private StructType structType =
            new StructType(
                    new StructField[] {
                        new StructField("a", DataTypes.BooleanType, false, Metadata.empty()),
                        new StructField("b", DataTypes.ByteType, false, Metadata.empty()),
                        new StructField("c", DataTypes.ShortType, true, Metadata.empty()),
                        new StructField("d", DataTypes.IntegerType, true, Metadata.empty()),
                        new StructField("e", DataTypes.LongType, true, Metadata.empty()),
                        new StructField("f", DataTypes.FloatType, true, Metadata.empty()),
                        new StructField("g", DataTypes.DoubleType, true, Metadata.empty()),
                        new StructField("h", new CharType(1), true, Metadata.empty()),
                        new StructField("i", DataTypes.StringType, true, Metadata.empty()),
                        new StructField(
                                "j", DataTypes.createDecimalType(10, 2), true, Metadata.empty()),
                        new StructField("k", DataTypes.BinaryType, true, Metadata.empty()),
                        new StructField("l", DataTypes.DateType, true, Metadata.empty()),
                        new StructField("m", DataTypes.TimestampType, true, Metadata.empty()),
                        new StructField("n", DataTypes.TimestampNTZType, true, Metadata.empty())
                    });

    @ParameterizedTest
    @ValueSource(strings = {"INDEXED", "COMPACTED"})
    void testConverter(KvFormat kvFormat) throws Exception {
        DataType[] dataTypes =
                Arrays.stream(structType.fields())
                        .map(SparkConversions::toFlussType)
                        .collect(Collectors.toList())
                        .toArray(new DataType[0]);

        try (SparkRowToFlussRowConverter converter =
                SparkRowToFlussRowConverter.create(structType, dataTypes, kvFormat)) {
            InternalRow internalRow = converter.toInternalRow(genSparkInternalRowForAllType());
            assertThat(internalRow.getFieldCount()).isEqualTo(14);
            assertThat(internalRow.getBoolean(0)).isTrue();
            assertThat(internalRow.getByte(1)).isEqualTo((byte) 2);
            assertThat(internalRow.isNullAt(2)).isTrue();
            assertThat(internalRow.getInt(3)).isEqualTo(100);
            assertThat(internalRow.getLong(4))
                    .isEqualTo(new BigInteger("12345678901234567890").longValue());
            assertThat(internalRow.getFloat(5)).isEqualTo(13.2f);
            assertThat(internalRow.getDouble(6)).isEqualTo(15.21);
            assertThat(internalRow.getChar(7, 1).toString()).isEqualTo("a");
            assertThat(internalRow.getString(8).toString()).isEqualTo("hello");
            assertThat(internalRow.getDecimal(9, 10, 2))
                    .isEqualTo(
                            com.alibaba.fluss.row.Decimal.fromBigDecimal(
                                    new BigDecimal("123456.789456123"), 10, 2));
            assertThat(internalRow.getBytes(10)).isEqualTo("1234567890".getBytes());
            assertThat(internalRow.getInt(11)).isEqualTo(12345);
            assertThat(internalRow.getTimestampLtz(12, 6))
                    .isEqualTo(TimestampLtz.fromEpochMicros(1735446630123456L));
            assertThat(internalRow.getTimestampNtz(13, 6))
                    .isEqualTo(TimestampNtz.fromMillis(1735446630123L, 456000));
        }
    }

    private org.apache.spark.sql.catalyst.InternalRow genSparkInternalRowForAllType() {
        org.apache.spark.sql.catalyst.InternalRow sparkRow = new SpecificInternalRow(structType);
        // set value
        sparkRow.setBoolean(0, true);
        sparkRow.setByte(1, (byte) 2);
        sparkRow.setNullAt(2);
        sparkRow.setInt(3, 100);
        sparkRow.setLong(4, new BigInteger("12345678901234567890").longValue());
        sparkRow.setFloat(5, 13.2f);
        sparkRow.setDouble(6, 15.21);
        sparkRow.update(7, UTF8String.fromString("a"));
        sparkRow.update(8, UTF8String.fromString("hello"));
        sparkRow.setDecimal(9, Decimal.apply("123456.789456123"), 10);
        sparkRow.update(10, "1234567890".getBytes());
        sparkRow.setInt(11, 12345);
        sparkRow.setLong(12, 1735446630123456L);
        sparkRow.setLong(13, 1735446630123456L);
        return sparkRow;
    }
}
