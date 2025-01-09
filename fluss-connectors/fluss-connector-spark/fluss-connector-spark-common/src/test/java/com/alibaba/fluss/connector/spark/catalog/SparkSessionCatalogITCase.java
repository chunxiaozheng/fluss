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

package com.alibaba.fluss.connector.spark.catalog;

import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.connector.spark.SparkTestBase;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** IT case for {@link com.alibaba.fluss.connector.spark.catalog.SparkSessionCatalog}. */
public class SparkSessionCatalogITCase extends SparkTestBase {

    // hide the beforeAll method of the parent class
    @BeforeAll
    public static void beforeAll1() {
        Configuration flussConf = FLUSS_CLUSTER_EXTENSION.getClientConfig();
        Map<String, String> configs = getSparkConfigs(flussConf);
        SparkConf sparkConf =
                new SparkConf().setAppName("bss-spark-unit-tests").setMaster("local[*]");
        configs.forEach(sparkConf::set);
        spark = SparkSession.builder().config(sparkConf).getOrCreate();
        spark.sparkContext().setLogLevel("WARN");
    }

    @Test
    public void createDatabaseTest() {
        sql("CREATE DATABASE IF NOT EXISTS " + DB + " WITH DBPROPERTIES ('provider' = 'fluss')");
        List<String> databases =
                sql("SHOW DATABASES").collectAsList().stream()
                        .map(row -> row.getString(0))
                        .collect(Collectors.toList());
        assertThat(databases.size()).isEqualTo(3);
        assertThat("default").isIn(databases);
        assertThat("fluss").isIn(databases);
        assertThat(DB).isIn(databases);
    }

    @Test
    public void createTableTest() {
        sql("CREATE DATABASE IF NOT EXISTS " + DB);
        sql(
                "CREATE TABLE IF NOT EXISTS "
                        + DB
                        + "."
                        + TABLE
                        + " (id INT, name STRING) USING fluss");
        List<String> tables =
                sql("SHOW TABLES IN " + DB).collectAsList().stream()
                        .map(row -> row.getString(1))
                        .collect(Collectors.toList());
        assertThat(tables.size()).isEqualTo(1);
        assertThat(tables.get(0)).isEqualTo(TABLE);
    }

    private static Map<String, String> getSparkConfigs(Configuration flussConf) {
        Map<String, String> configs = new HashMap<>();
        configs.put("spark.sql.catalog.spark_catalog", SparkSessionCatalog.class.getName());
        configs.put(
                "spark.sql.catalog.spark_catalog.bootstrap.servers",
                String.join(",", flussConf.get(ConfigOptions.BOOTSTRAP_SERVERS)));
        return configs;
    }
}
