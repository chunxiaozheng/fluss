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

package com.alibaba.fluss.connector.spark.table;

import com.alibaba.fluss.config.ConfigOption;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.connector.spark.SparkConnectorOptions;
import com.alibaba.fluss.connector.spark.sink.FlussWriteBuilder;
import com.alibaba.fluss.connector.spark.utils.SparkConversions;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.shaded.guava32.com.google.common.collect.ImmutableSet;

import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.SupportsWrite;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/** spark table. */
public class SparkTable implements Table, SupportsWrite, SupportsRead {

    private static final Set<TableCapability> CAPABILITIES =
            ImmutableSet.of(TableCapability.BATCH_WRITE);

    private final TablePath tablePath;
    private final TableDescriptor tableDescriptor;
    private final StructType sparkSchema;
    private final Transform[] transforms;
    private final Map<String, String> properties;
    private final String bootstrapServers;

    public SparkTable(
            TablePath tablePath, TableDescriptor tableDescriptor, String bootstrapServers) {
        this.tablePath = tablePath;
        this.tableDescriptor = tableDescriptor;
        this.sparkSchema = SparkConversions.toSparkSchema(tableDescriptor.getSchema());
        this.transforms = SparkConversions.toSparkTransforms(tableDescriptor.getPartitionKeys());
        this.properties = new HashMap<>();
        this.properties.putAll(tableDescriptor.getProperties());
        this.properties.putAll(tableDescriptor.getCustomProperties());
        this.bootstrapServers = bootstrapServers;
    }

    @Override
    public Transform[] partitioning() {
        return transforms;
    }

    @Override
    public Map<String, String> properties() {
        return properties;
    }

    @Override
    public String name() {
        return tablePath.getTableName();
    }

    @Override
    public StructType schema() {
        return sparkSchema;
    }

    @Override
    public Set<TableCapability> capabilities() {
        return CAPABILITIES;
    }

    @Override
    public ScanBuilder newScanBuilder(CaseInsensitiveStringMap caseInsensitiveStringMap) {
        return null;
    }

    @Override
    public WriteBuilder newWriteBuilder(LogicalWriteInfo logicalWriteInfo) {
        return new FlussWriteBuilder(
                tablePath, sparkSchema, tableDescriptor, toFlussClientConfig());
    }

    private Configuration toFlussClientConfig() {
        Configuration flussConfig = new Configuration();
        flussConfig.setString(ConfigOptions.BOOTSTRAP_SERVERS.key(), bootstrapServers);

        // forward all client configs
        for (ConfigOption<?> option : SparkConnectorOptions.CLIENT_OPTIONS) {
            if (properties.containsKey(option.key())) {
                flussConfig.setString(option.key(), properties.get(option.key()));
            }
        }
        return flussConfig;
    }
}
