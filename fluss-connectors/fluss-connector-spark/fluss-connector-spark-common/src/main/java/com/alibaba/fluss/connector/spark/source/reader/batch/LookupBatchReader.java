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

import com.alibaba.fluss.client.table.LookupResult;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.metadata.KvFormat;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.row.BinaryString;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.row.encode.RowEncoder;
import com.alibaba.fluss.types.DataType;

import org.apache.spark.sql.sources.EqualTo;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** A lookup batch reader for fluss primary key table. */
public class LookupBatchReader extends SparkBaseBatchReader {

    private static final Logger LOG = LoggerFactory.getLogger(LookupBatchReader.class);

    private final Filter[] filters;
    private boolean init;

    public LookupBatchReader(
            TablePath tablePath,
            StructType sparkSchema,
            Configuration flussConfig,
            Filter[] filters) {
        super(tablePath, sparkSchema, flussConfig, null);
        if (!table.getDescriptor().hasPrimaryKey()) {
            throw new UnsupportedOperationException(
                    String.format(
                            "Only primary key table support lookup, %s is a log table",
                            tablePath.toString()));
        }
        this.filters = filters;
    }

    @Override
    public boolean next() throws IOException {
        // only send lookup request when unInit
        if (!init) {
            try {
                LookupResult lookupResult =
                        table.lookup(generateLookupRow(table.getDescriptor())).get();
                if (lookupResult.getRow() != null) {
                    currentRow = dataConverter.toInternalRow(lookupResult.getRow());
                    return true;
                }
            } catch (Exception e) {
                LOG.error("lookup table {} failed.", tablePath, e);
                throw new IOException("lookup failed", e);
            } finally {
                init = true;
            }
        }
        return false;
    }

    private InternalRow generateLookupRow(TableDescriptor tableDescriptor) throws Exception {
        // construct values by primary key order
        Map<String, Object> valueMap =
                Arrays.stream(filters)
                        .collect(
                                Collectors.toMap(
                                        filter -> ((EqualTo) filter).attribute(),
                                        filter -> ((EqualTo) filter).value()));
        List<Object> values = new ArrayList<>();
        for (String pkName : tableDescriptor.getSchema().getPrimaryKey().get().getColumnNames()) {
            values.add(valueMap.get(pkName));
        }

        int[] pkIndic = tableDescriptor.getSchema().getPrimaryKeyIndexes();
        DataType[] allDataTypes =
                tableDescriptor.getSchema().toRowType().getChildren().toArray(new DataType[0]);
        DataType[] pkDataTypes = new DataType[pkIndic.length];
        for (int i = 0; i < pkIndic.length; i++) {
            pkDataTypes[i] = allDataTypes[pkIndic[i]];
        }
        try (RowEncoder rowEncoder = RowEncoder.create(KvFormat.INDEXED, pkDataTypes)) {
            rowEncoder.startNewRow();
            for (int i = 0; i < pkDataTypes.length; i++) {
                // TODO: support other types
                rowEncoder.encodeField(
                        i,
                        values.get(i) instanceof String
                                ? BinaryString.fromString((String) values.get(i))
                                : values.get(i));
            }
            return rowEncoder.finishRow();
        }
    }
}
