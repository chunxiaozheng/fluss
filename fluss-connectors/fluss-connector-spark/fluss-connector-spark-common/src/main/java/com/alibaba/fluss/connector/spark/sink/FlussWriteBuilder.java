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

import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TablePath;

import org.apache.spark.sql.connector.write.Write;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.types.StructType;

/** Spark's {@link WriteBuilder} implementation for Fluss. */
public class FlussWriteBuilder implements WriteBuilder {

    private final TablePath tablePath;
    private final StructType sparkSchema;
    private final TableDescriptor tableDescriptor;
    private final Configuration flussConfig;

    public FlussWriteBuilder(
            TablePath tablePath,
            StructType sparkSchema,
            TableDescriptor tableDescriptor,
            Configuration flussConfig) {
        this.tablePath = tablePath;
        this.sparkSchema = sparkSchema;
        this.tableDescriptor = tableDescriptor;
        this.flussConfig = flussConfig;
    }

    @Override
    public Write build() {
        return new FlussWrite(tablePath, sparkSchema, tableDescriptor, flussConfig);
    }
}
