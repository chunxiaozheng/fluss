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

import com.alibaba.fluss.client.scanner.ScanRecord;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.connector.spark.source.FlussInputPartition;
import com.alibaba.fluss.metadata.TablePath;

import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/** A limit scan batch reader for fluss table. */
public class LimitScanBatchReader extends SparkBaseBatchReader {

    private static final Logger LOG = LoggerFactory.getLogger(LimitScanBatchReader.class);

    private static final int LIMIT_CEIL = 1000;

    private final FlussInputPartition inputPartition;
    private final int limit;

    private boolean init;
    private Iterator<ScanRecord> recordIterator;

    public LimitScanBatchReader(
            TablePath tablePath,
            StructType sparkSchema,
            Configuration flussConfig,
            FlussInputPartition inputPartition,
            int limit,
            @Nullable int[] projectedFields) {
        super(tablePath, sparkSchema, flussConfig, projectedFields);
        if (limit > LIMIT_CEIL) {
            throw new UnsupportedOperationException(
                    String.format("LIMIT statement doesn't support greater than %s", LIMIT_CEIL));
        }
        this.inputPartition = inputPartition;
        this.limit = limit;
    }

    @Override
    public boolean next() throws IOException {
        // only send limitScan request when unInit
        if (!init) {
            try {
                List<ScanRecord> records =
                        table.limitScan(inputPartition.getTableBucket(), limit, projectedFields)
                                .get();
                recordIterator = records == null ? null : records.iterator();
            } catch (Exception e) {
                LOG.error("limit scan table {} failed", tablePath, e);
                throw new IOException("limit scan failed", e);
            } finally {
                init = true;
            }
        }

        if (recordIterator != null && recordIterator.hasNext()) {
            currentRow = dataConverter.toInternalRow(recordIterator.next().getRow());
            return true;
        }
        return false;
    }
}
