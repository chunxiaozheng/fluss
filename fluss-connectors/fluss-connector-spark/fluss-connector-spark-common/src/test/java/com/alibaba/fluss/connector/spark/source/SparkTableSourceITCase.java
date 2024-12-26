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

import com.alibaba.fluss.connector.spark.SparkTestBase;

import org.apache.spark.sql.Row;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** IT case for spark source. */
public class SparkTableSourceITCase extends SparkTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(SparkTableSourceITCase.class);

    private static final String DATA =
            "(%s, 1, 1, '%s', true, 1, 1.1, 1.111, "
                    + "'abc', X'010203', date'2024-12-26', "
                    + "timestamp'2024-12-26 12:34:56.123456', "
                    + "timestamp'2024-12-26 12:34:56.123456', 1.1111111)";

    private static final String LOOKUP_SQL =
            "SELECT * FROM fluss_catalog." + DB + "." + TABLE + " WHERE a = %s AND d = '%s'";
    private static final String LIMIT_SQL =
            "SELECT %s FROM fluss_catalog." + DB + "." + TABLE + " LIMIT %s";

    private final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd");
    private final LocalDate currentDate = LocalDate.now();

    @ParameterizedTest
    @CsvSource({"true, false", "true, true", "false, false", "false, true"})
    public void lookupTest(boolean isPrimaryKeyTable, boolean partitioned) {
        String options = getTableOptions(isPrimaryKeyTable, partitioned);
        sql("CREATE DATABASE IF NOT EXISTS fluss_catalog." + DB);
        sql(CREATE_TABLE_SQL + options);
        insertData(2, partitioned ? 4 : 1);

        if (isPrimaryKeyTable) {
            List<Row> rows =
                    sql(String.format(LOOKUP_SQL, 0, currentDate.format(formatter)))
                            .collectAsList();
            assertThat(rows).hasSize(1);
            assertThat(rows.get(0).getInt(0)).isEqualTo(0);
            assertThat(rows.get(0).getShort(1)).isEqualTo((short) 1);
            assertThat(rows.get(0).getByte(2)).isEqualTo((byte) 1);
            assertThat(rows.get(0).getString(3)).isEqualTo(currentDate.format(formatter));
            assertThat(rows.get(0).getBoolean(4)).isTrue();
            assertThat(rows.get(0).getLong(5)).isEqualTo(1L);
            assertThat(rows.get(0).getFloat(6)).isEqualTo(1.1f);
            assertThat(rows.get(0).getDouble(7)).isEqualTo(1.111);
            assertThat(rows.get(0).getString(8)).isEqualTo("abc");
            assertThat(rows.get(0).get(9)).isEqualTo(new byte[] {1, 2, 3});
            // java.sql.Date
            assertThat(rows.get(0).get(10).toString()).isEqualTo("2024-12-26");
            // java.sql.Timestamp
            assertThat(rows.get(0).get(11).toString()).isEqualTo("2024-12-26 12:34:56.123456");
            // java.time.LocalDateTime
            assertThat(rows.get(0).get(12).toString()).isEqualTo("2024-12-26T12:34:56.123456");
            assertThat(rows.get(0).getDecimal(13)).isEqualTo(new BigDecimal("1.11111"));

            // upsert test
            sql(
                    "INSERT INTO fluss_catalog."
                            + DB
                            + "."
                            + TABLE
                            + " VALUES (0, 10, 10, '"
                            + currentDate.format(formatter)
                            + "', false, 10, 11.1, 11.111, 'def', X'040506', "
                            + "date'2024-12-28', timestamp'2024-12-28 22:34:56.123456', "
                            + "timestamp'2024-12-28 22:34:56.123456', 6.66666666)");

            rows = sql(String.format(LOOKUP_SQL, 0, currentDate.format(formatter))).collectAsList();
            assertThat(rows).hasSize(1);
            assertThat(rows.get(0).getInt(0)).isEqualTo(0);
            assertThat(rows.get(0).getShort(1)).isEqualTo((short) 10);
            assertThat(rows.get(0).getByte(2)).isEqualTo((byte) 10);
            assertThat(rows.get(0).getString(3)).isEqualTo(currentDate.format(formatter));
            assertThat(rows.get(0).getBoolean(4)).isFalse();
            assertThat(rows.get(0).getLong(5)).isEqualTo(10L);
            assertThat(rows.get(0).getFloat(6)).isEqualTo(11.1f);
            assertThat(rows.get(0).getDouble(7)).isEqualTo(11.111);
            assertThat(rows.get(0).getString(8)).isEqualTo("def");
            assertThat(rows.get(0).get(9)).isEqualTo(new byte[] {4, 5, 6});
            // java.sql.Date
            assertThat(rows.get(0).get(10).toString()).isEqualTo("2024-12-28");
            // java.sql.Timestamp
            assertThat(rows.get(0).get(11).toString()).isEqualTo("2024-12-28 22:34:56.123456");
            // java.time.LocalDateTime
            assertThat(rows.get(0).get(12).toString()).isEqualTo("2024-12-28T22:34:56.123456");
            assertThat(rows.get(0).getDecimal(13)).isEqualTo(new BigDecimal("6.66667"));

            // test query non-exist row
            rows =
                    sql(String.format(LOOKUP_SQL, 0, currentDate.plusDays(1).format(formatter)))
                            .collectAsList();
            assertThat(rows).isEmpty();
        } else {
            assertThatThrownBy(
                            () -> sql(String.format(LOOKUP_SQL, 0, currentDate.format(formatter))))
                    .cause()
                    .isInstanceOf(UnsupportedOperationException.class)
                    .message()
                    .isEqualTo(
                            "fluss primary key table support lookup or limit scan, "
                                    + "log table only support limit scan in batch mode now, "
                                    + "my_db.my_table is a log table");
        }
    }

    @ParameterizedTest
    @CsvSource({"true, false", "true, true", "false, false", "false, true"})
    public void limitScanTest(boolean isPrimaryKeyTable, boolean partitioned) {
        String options = getTableOptions(isPrimaryKeyTable, partitioned);
        sql("CREATE DATABASE IF NOT EXISTS fluss_catalog." + DB);
        sql(CREATE_TABLE_SQL + options);
        insertData(15, partitioned ? 4 : 1);

        List<Row> rows = sql(String.format(LIMIT_SQL, "*", 20)).collectAsList();
        assertThat(rows).hasSize(15);
        Set<Integer> ids = rows.stream().map(row -> row.getInt(0)).collect(Collectors.toSet());
        assertThat(ids).hasSize(15);
        assertThat(ids)
                .containsOnlyElementsOf(IntStream.range(0, 15).boxed().collect(Collectors.toSet()));

        // upsert test
        sql(
                "INSERT INTO fluss_catalog."
                        + DB
                        + "."
                        + TABLE
                        + " VALUES (0, 10, 10, '"
                        + currentDate.format(formatter)
                        + "', false, 10, 11.1, 11.111, 'def', X'040506', "
                        + "date'2024-12-28', timestamp'2024-12-28 22:34:56.123456', "
                        + "timestamp'2024-12-28 22:34:56.123456', 6.66666666)");
        rows = sql(String.format(LIMIT_SQL, "*", 20)).collectAsList();
        if (isPrimaryKeyTable) {
            assertThat(rows).hasSize(15);
        } else {
            assertThat(rows).hasSize(16);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void projectedFieldsTest(boolean isPrimaryKeyTable) {
        String options = getTableOptions(isPrimaryKeyTable, true);
        sql("CREATE DATABASE IF NOT EXISTS fluss_catalog." + DB);
        sql(CREATE_TABLE_SQL + options);
        insertData(5, 4);

        // project fields test
        List<Row> rows = sql(String.format(LIMIT_SQL, "a, e, i, j, m", 20)).collectAsList();
        assertThat(rows).hasSize(5);
        assertThat(rows.get(0).getBoolean(1)).isTrue();
        assertThat(rows.get(0).getString(2)).isEqualTo("abc");
        assertThat(rows.get(0).get(3)).isEqualTo(new byte[] {1, 2, 3});
        assertThat(rows.get(0).get(4).toString()).isEqualTo("2024-12-26T12:34:56.123456");

        // project fields test out of order
        rows = sql(String.format(LIMIT_SQL, "d, b, n, l, f", 20)).collectAsList();
        assertThat(rows).hasSize(5);
        assertThat(rows.get(0).getShort(1)).isEqualTo((short) 1);
        assertThat(rows.get(0).getDecimal(2)).isEqualTo(new BigDecimal("1.11111"));
        assertThat(rows.get(0).get(3).toString()).isEqualTo("2024-12-26 12:34:56.123456");
        assertThat(rows.get(0).getLong(4)).isEqualTo(1L);

        // all
        rows =
                sql(String.format(LIMIT_SQL, "a, b, c, d, e, f, g, h, i, j, k, l, m, n", 20))
                        .collectAsList();
        assertThat(rows).hasSize(5);
        assertThat(rows.get(0).getShort(1)).isEqualTo((short) 1);
        assertThat(rows.get(0).getByte(2)).isEqualTo((byte) 1);
        assertThat(rows.get(0).getBoolean(4)).isTrue();
        assertThat(rows.get(0).getLong(5)).isEqualTo(1L);
        assertThat(rows.get(0).getFloat(6)).isEqualTo(1.1f);
        assertThat(rows.get(0).getDouble(7)).isEqualTo(1.111);
        assertThat(rows.get(0).getString(8)).isEqualTo("abc");
        assertThat(rows.get(0).get(9)).isEqualTo(new byte[] {1, 2, 3});
        assertThat(rows.get(0).get(10).toString()).isEqualTo("2024-12-26");
        assertThat(rows.get(0).get(11).toString()).isEqualTo("2024-12-26 12:34:56.123456");
        assertThat(rows.get(0).get(12).toString()).isEqualTo("2024-12-26T12:34:56.123456");
        assertThat(rows.get(0).getDecimal(13)).isEqualTo(new BigDecimal("1.11111"));

        // all with reversed
        rows =
                sql(String.format(LIMIT_SQL, "n, m, l, k, j, i, h, g, f, e, d, c, b, a", 20))
                        .collectAsList();
        assertThat(rows).hasSize(5);
        assertThat(rows.get(0).getDecimal(0)).isEqualTo(new BigDecimal("1.11111"));
        assertThat(rows.get(0).get(1).toString()).isEqualTo("2024-12-26T12:34:56.123456");
        assertThat(rows.get(0).get(2).toString()).isEqualTo("2024-12-26 12:34:56.123456");
        assertThat(rows.get(0).get(3).toString()).isEqualTo("2024-12-26");
        assertThat(rows.get(0).get(4)).isEqualTo(new byte[] {1, 2, 3});
        assertThat(rows.get(0).getString(5)).isEqualTo("abc");
        assertThat(rows.get(0).getDouble(6)).isEqualTo(1.111);
        assertThat(rows.get(0).getFloat(7)).isEqualTo(1.1f);
        assertThat(rows.get(0).getLong(8)).isEqualTo(1L);
        assertThat(rows.get(0).getBoolean(9)).isTrue();
        assertThat(rows.get(0).getByte(11)).isEqualTo((byte) 1);
        assertThat(rows.get(0).getShort(12)).isEqualTo((short) 1);
    }

    private String getTableOptions(boolean isPrimaryKeyTable, boolean partitioned) {
        String pkOptions = isPrimaryKeyTable ? ", 'primary.key' = 'a,d'" : "";
        return partitioned
                ? " PARTITIONED BY (d) OPTIONS ("
                        + "'table.auto-partition.enabled' = 'true', "
                        + "'table.auto-partition.time-unit' = 'day', "
                        + "'bucket.num' = '3'"
                        + pkOptions
                        + ")"
                : " OPTIONS ('bucket.num' = '3'" + pkOptions + ")";
    }

    private void insertData(int count, int partitionCount) {
        long begin = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            String insertData =
                    String.format(
                            DATA, i, currentDate.plusDays(i % partitionCount).format(formatter));
            sql("INSERT INTO fluss_catalog." + DB + "." + TABLE + " VALUES " + insertData);
        }
        LOG.info(
                "insert {} data into {} cost {}ms",
                count,
                TABLE,
                System.currentTimeMillis() - begin);
    }
}
