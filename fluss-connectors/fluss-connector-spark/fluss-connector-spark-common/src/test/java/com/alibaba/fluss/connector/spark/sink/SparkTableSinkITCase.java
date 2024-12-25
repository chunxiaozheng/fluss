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

package com.alibaba.fluss.connector.spark.sink;

import com.alibaba.fluss.client.scanner.ScanRecord;
import com.alibaba.fluss.client.scanner.log.LogScan;
import com.alibaba.fluss.client.scanner.log.LogScanner;
import com.alibaba.fluss.client.scanner.log.ScanRecords;
import com.alibaba.fluss.client.table.Table;
import com.alibaba.fluss.connector.spark.SparkTestBase;
import com.alibaba.fluss.metadata.PartitionInfo;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.record.RowKind;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nullable;

import java.time.Duration;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** IT case for spark sink. */
public class SparkTableSinkITCase extends SparkTestBase {

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void writeTest(boolean isPrimaryKeyTable) {
        String primaryKeyOption = isPrimaryKeyTable ? " OPTIONS ('primary.key' = 'a')" : "";
        sql("CREATE DATABASE IF NOT EXISTS fluss_catalog." + DB);
        sql(CREATE_TABLE_SQL + primaryKeyOption);
        sql(
                "INSERT INTO fluss_catalog."
                        + DB
                        + "."
                        + TABLE
                        + " VALUES "
                        + "(1, 1, 1, 'string1', true, 1, 1.1, 1.111, 'abc', X'010203', "
                        + "date'2024-12-26', timestamp'2024-12-26 12:34:56.123456', "
                        + "timestamp'2024-12-26 12:34:56.123456', 1.1111111), "
                        + "(2, 2, 2, 'string2', false, 2, 2.2, 2.222, 'def', X'040506', "
                        + "date'2024-12-27', timestamp'2024-12-27 22:34:56.123456', "
                        + "timestamp'2024-12-27 22:34:56.123456', 2.2222222)");

        // due to don't support batch read now, use log scanner to assert write result
        List<ScanRecord> result = scan(2, null, 0);

        // assert
        assertThat(result).hasSize(2);
        ScanRecord record0 = result.get(0).getRow().getInt(0) == 1 ? result.get(0) : result.get(1);
        ScanRecord record1 = result.get(0).getRow().getInt(0) == 2 ? result.get(0) : result.get(1);
        assertThat(record0.getRow().toString())
                .isEqualTo(
                        "(1,1,1,string1,true,1,1.1,1.111,abc,[1, 2, 3],20083,2024-12-26T04:34:56.123456Z,2024-12-26T12:34:56.123456,1.11111)");
        assertThat(record1.getRow().toString())
                .isEqualTo(
                        "(2,2,2,string2,false,2,2.2,2.222,def,[4, 5, 6],20084,2024-12-27T14:34:56.123456Z,2024-12-27T22:34:56.123456,2.22222)");
        assertThat(record0.getRowKind())
                .isEqualTo(isPrimaryKeyTable ? RowKind.INSERT : RowKind.APPEND_ONLY);
        assertThat(record1.getRowKind())
                .isEqualTo(isPrimaryKeyTable ? RowKind.INSERT : RowKind.APPEND_ONLY);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void partitionedWriteTest(boolean isPrimaryKeyTable) {
        // use string type as partition column
        String partitionOptions = " PARTITIONED BY (d)";
        String primaryKeyOption =
                isPrimaryKeyTable
                        ? " OPTIONS ('primary.key' = 'a,d', 'table.auto-partition.enabled' = 'true', 'table.auto-partition.time-unit' = 'day')"
                        : " OPTIONS ('table.auto-partition.enabled' = 'true', 'table.auto-partition.time-unit' = 'day')";
        sql("CREATE DATABASE IF NOT EXISTS fluss_catalog." + DB);
        sql(CREATE_TABLE_SQL + partitionOptions + primaryKeyOption);

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd");
        LocalDate currentDate = LocalDate.now();
        String currentDateStr = currentDate.format(formatter);
        String nextDayStr = currentDate.plusDays(1).format(formatter);

        sql(
                "INSERT INTO fluss_catalog."
                        + DB
                        + "."
                        + TABLE
                        + " VALUES "
                        + "(1, 1, 1, '"
                        + currentDateStr
                        + "', true, 1, 1.1, 1.111, 'abc', X'010203', "
                        + "date'2024-12-26', timestamp'2024-12-26 12:34:56.123456', "
                        + "timestamp'2024-12-26 12:34:56.123456', 1.1111111), "
                        + "(2, 2, 2, '"
                        + nextDayStr
                        + "', false, 2, 2.2, 2.222, 'def', X'040506', "
                        + "date'2024-12-27', timestamp'2024-12-27 22:34:56.123456', "
                        + "timestamp'2024-12-27 22:34:56.123456', 2.2222222)");
        // get partition list
        List<PartitionInfo> partitionInfos =
                admin.listPartitionInfos(TablePath.of(DB, TABLE)).join();
        partitionInfos.sort((p1, p2) -> (int) (p1.getPartitionId() - p2.getPartitionId()));
        assertThat(partitionInfos).hasSize(4);

        List<ScanRecord> partition0Result = scan(1, partitionInfos.get(0).getPartitionId(), 0);
        List<ScanRecord> partition1Result = scan(1, partitionInfos.get(1).getPartitionId(), 0);

        // assert
        assertThat(partition0Result).hasSize(1);
        assertThat(partition1Result).hasSize(1);

        assertThat(partition0Result.get(0).getRow().toString())
                .isEqualTo(
                        "(1,1,1,"
                                + currentDateStr
                                + ",true,1,1.1,1.111,abc,[1, 2, 3],20083,2024-12-26T04:34:56.123456Z,2024-12-26T12:34:56.123456,1.11111)");
        assertThat(partition1Result.get(0).getRow().toString())
                .isEqualTo(
                        "(2,2,2,"
                                + nextDayStr
                                + ",false,2,2.2,2.222,def,[4, 5, 6],20084,2024-12-27T14:34:56.123456Z,2024-12-27T22:34:56.123456,2.22222)");
        assertThat(partition0Result.get(0).getRowKind())
                .isEqualTo(isPrimaryKeyTable ? RowKind.INSERT : RowKind.APPEND_ONLY);
        assertThat(partition1Result.get(0).getRowKind())
                .isEqualTo(isPrimaryKeyTable ? RowKind.INSERT : RowKind.APPEND_ONLY);
    }

    @Test
    public void upsertTest() {
        sql("CREATE DATABASE IF NOT EXISTS fluss_catalog." + DB);
        sql(CREATE_TABLE_SQL + " OPTIONS ('primary.key' = 'a')");

        sql(
                "INSERT INTO fluss_catalog."
                        + DB
                        + "."
                        + TABLE
                        + " VALUES "
                        + "(1, 1, 1, 'string1', true, 1, 1.1, 1.111, 'abc', X'010203', "
                        + "date'2024-12-26', timestamp'2024-12-26 12:34:56.123456', "
                        + "timestamp'2024-12-26 12:34:56.123456', 1.1111111), "
                        + "(2, 2, 2, 'string2', false, 2, 2.2, 2.222, 'def', X'040506', "
                        + "date'2024-12-27', timestamp'2024-12-27 22:34:56.123456', "
                        + "timestamp'2024-12-27 22:34:56.123456', 2.2222222)");

        List<ScanRecord> records = limitScan(10, null, 0);
        assertThat(records).hasSize(2);
        ScanRecord record0 =
                records.get(0).getRow().getInt(0) == 1 ? records.get(0) : records.get(1);
        ScanRecord record1 =
                records.get(0).getRow().getInt(0) == 2 ? records.get(0) : records.get(1);
        assertThat(record0.getRow().toString())
                .isEqualTo(
                        "(1,1,1,string1,true,1,1.1,1.111,abc,[1, 2, 3],20083,2024-12-26T04:34:56.123456Z,2024-12-26T12:34:56.123456,1.11111)");
        assertThat(record1.getRow().toString())
                .isEqualTo(
                        "(2,2,2,string2,false,2,2.2,2.222,def,[4, 5, 6],20084,2024-12-27T14:34:56.123456Z,2024-12-27T22:34:56.123456,2.22222)");

        // upsert
        sql(
                "INSERT INTO fluss_catalog."
                        + DB
                        + "."
                        + TABLE
                        + " VALUES "
                        + "(1, 10, 10, 'new1', false, 10, 1.1, 1.111, 'hij', X'040506', "
                        + "date'2024-12-28', timestamp'2024-12-28 12:34:56.123456', "
                        + "timestamp'2024-12-28 12:34:56.123456', 6.66666666)");

        records = limitScan(10, null, 0);
        assertThat(records).hasSize(2);
        record0 = records.get(0).getRow().getInt(0) == 1 ? records.get(0) : records.get(1);
        record1 = records.get(0).getRow().getInt(0) == 2 ? records.get(0) : records.get(1);
        assertThat(record0.getRow().toString())
                .isEqualTo(
                        "(1,10,10,new1,false,10,1.1,1.111,hij,[4, 5, 6],20085,2024-12-28T04:34:56.123456Z,2024-12-28T12:34:56.123456,6.66667)");
        assertThat(record1.getRow().toString())
                .isEqualTo(
                        "(2,2,2,string2,false,2,2.2,2.222,def,[4, 5, 6],20084,2024-12-27T14:34:56.123456Z,2024-12-27T22:34:56.123456,2.22222)");
    }

    private List<ScanRecord> scan(int totalCount, @Nullable Long partitionId, int bucketId) {
        Table table = connection.getTable(TablePath.of(DB, TABLE));
        int count = 0;
        LogScanner scanner = table.getLogScanner(new LogScan());
        if (partitionId == null) {
            scanner.subscribeFromBeginning(bucketId);
        } else {
            scanner.subscribeFromBeginning(partitionId, bucketId);
        }

        List<ScanRecord> result = new ArrayList<>();
        while (count < totalCount) {
            ScanRecords records = scanner.poll(Duration.ofSeconds(2));
            count += records.count();
            if (!records.isEmpty()) {
                for (ScanRecord record : records) {
                    result.add(record);
                }
            }
        }
        return result;
    }

    private List<ScanRecord> limitScan(int limit, @Nullable Long partitionId, int bucketId) {
        TablePath tablePath = TablePath.of(DB, TABLE);
        Table table = connection.getTable(tablePath);
        long tableId = admin.getTable(tablePath).join().getTableId();
        TableBucket bucket = new TableBucket(tableId, partitionId, bucketId);
        return table.limitScan(bucket, limit, null).join();
    }
}
