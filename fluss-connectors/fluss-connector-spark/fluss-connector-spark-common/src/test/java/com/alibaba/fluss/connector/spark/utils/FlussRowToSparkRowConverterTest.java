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
import com.alibaba.fluss.record.RowKind;
import com.alibaba.fluss.row.indexed.IndexedRow;
import com.alibaba.fluss.row.indexed.IndexedRowWriter;
import com.alibaba.fluss.types.DataField;
import com.alibaba.fluss.types.DataType;
import com.alibaba.fluss.types.RowType;
import com.alibaba.fluss.utils.DateTimeUtils;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalTime;

import static com.alibaba.fluss.row.TestInternalRowGenerator.createAllRowType;
import static com.alibaba.fluss.row.TestInternalRowGenerator.createAllTypes;
import static com.alibaba.fluss.row.indexed.IndexedRowTest.genRecordForAllTypes;
import static org.assertj.core.api.Assertions.assertThat;

/** Unit test for {@link FlussRowToSparkRowConverter}. */
public class FlussRowToSparkRowConverterTest {

    @Test
    void testConverter() throws Exception {
        RowType rowType = createAllRowType();
        FlussRowToSparkRowConverter flussRowToSparkRowConverter =
                new FlussRowToSparkRowConverter(generateStructType(rowType), rowType.getChildren());

        IndexedRow row = new IndexedRow(rowType.getChildren().toArray(new DataType[0]));
        try (IndexedRowWriter writer = genRecordForAllTypes(createAllTypes())) {
            row.pointTo(writer.segment(), 0, writer.position());

            ScanRecord scanRecord = new ScanRecord(0, 1L, RowKind.UPDATE_BEFORE, row);

            InternalRow sparkRow = flussRowToSparkRowConverter.toInternalRow(scanRecord);
            assertThat(sparkRow.numFields()).isEqualTo(rowType.getFieldCount());

            // now, check the field values
            assertThat(sparkRow.getBoolean(0)).isTrue();
            assertThat(sparkRow.getByte(1)).isEqualTo((byte) 2);
            assertThat(sparkRow.getShort(2)).isEqualTo((short) 10);
            assertThat(sparkRow.getInt(3)).isEqualTo(100);
            assertThat(sparkRow.getLong(4))
                    .isEqualTo(new BigInteger("12345678901234567890").longValue());
            assertThat(sparkRow.getFloat(5)).isEqualTo(13.2f);
            assertThat(sparkRow.getDouble(6)).isEqualTo(15.21);

            assertThat(DateTimeUtils.toLocalDate(sparkRow.getInt(7)))
                    .isEqualTo(LocalDate.of(2023, 10, 25));
            // spark use int to represent time
            assertThat(DateTimeUtils.toLocalTime(sparkRow.getInt(8)))
                    .isEqualTo(LocalTime.of(9, 30, 0, 0));

            assertThat(sparkRow.getBinary(9)).isEqualTo("1234567890".getBytes());
            assertThat(sparkRow.getBinary(10)).isEqualTo("20".getBytes());

            assertThat(sparkRow.getString(11).toString()).isEqualTo("1");
            assertThat(sparkRow.getString(12).toString()).isEqualTo("hello");

            assertThat(sparkRow.getDecimal(13, 5, 2).toJavaBigDecimal())
                    .isEqualTo(BigDecimal.valueOf(9, 2));
            assertThat(sparkRow.getDecimal(14, 20, 0).toJavaBigDecimal())
                    .isEqualTo(new BigDecimal(10));

            assertThat(sparkRow.getLong(15)).isEqualTo(1698235273182000L);
            assertThat(sparkRow.getLong(16)).isEqualTo(1698235273182000L);

            assertThat(sparkRow.getLong(17)).isEqualTo(1698235273182000L);
            assertThat(sparkRow.isNullAt(18)).isTrue();
        }
    }

    private StructType generateStructType(RowType rowType) {
        StructField[] structFields = new StructField[rowType.getFieldCount()];
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            DataField field = rowType.getFields().get(i);
            DataType flussDataType = field.getType();
            structFields[i] =
                    new StructField(
                            field.getName(),
                            SparkConversions.toSparkType(flussDataType),
                            flussDataType.isNullable(),
                            Metadata.empty());
            if (field.getDescription().isPresent()) {
                structFields[i] = structFields[i].withComment(field.getDescription().get());
            }
        }

        return new StructType(structFields);
    }
}
