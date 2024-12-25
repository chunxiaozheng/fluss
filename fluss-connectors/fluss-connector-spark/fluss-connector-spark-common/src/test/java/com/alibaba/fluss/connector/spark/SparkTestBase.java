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

package com.alibaba.fluss.connector.spark;

import com.alibaba.fluss.client.Connection;
import com.alibaba.fluss.client.ConnectionFactory;
import com.alibaba.fluss.client.admin.Admin;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.connector.spark.catalog.SparkCatalog;
import com.alibaba.fluss.server.testutils.FlussClusterExtension;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/** IT case base. */
public class SparkTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(SparkTestBase.class);

    protected static final String DB = "my_db";
    protected static final String TABLE = "my_table";

    protected static final String CREATE_TABLE_SQL =
            "CREATE TABLE IF NOT EXISTS fluss_catalog."
                    + DB
                    + "."
                    + TABLE
                    + " ("
                    + " a INT,"
                    + " b SHORT,"
                    + " c BYTE,"
                    + " d STRING,"
                    + " e BOOLEAN,"
                    + " f LONG,"
                    + " g FLOAT,"
                    + " h DOUBLE,"
                    + " i CHAR(3),"
                    + " j BINARY,"
                    + " k DATE,"
                    + " l TIMESTAMP,"
                    + " m TIMESTAMP_NTZ,"
                    + " n DECIMAL(10, 5)"
                    + ")";

    @RegisterExtension
    protected static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder().setNumOfTabletServers(1).build();

    protected static SparkSession spark;
    protected Connection connection;
    protected Admin admin;

    @BeforeAll
    public static void beforeAll() {
        Configuration flussConf = FLUSS_CLUSTER_EXTENSION.getClientConfig();
        Map<String, String> configs = getSparkConfigs(flussConf);
        SparkConf sparkConf =
                new SparkConf().setAppName("bss-spark-unit-tests").setMaster("local[*]");
        configs.forEach(sparkConf::set);
        spark = SparkSession.builder().config(sparkConf).getOrCreate();
        spark.sparkContext().setLogLevel("WARN");
    }

    @BeforeEach
    public void beforeEach() {
        connection = ConnectionFactory.createConnection(FLUSS_CLUSTER_EXTENSION.getClientConfig());
        admin = connection.getAdmin();
    }

    @AfterAll
    public static void afterAll() {
        try {
            spark.close();
        } catch (Exception e) {
            // ignore
        }
    }

    @AfterEach
    public void afterEach() {
        sql("DROP TABLE IF EXISTS fluss_catalog." + DB + "." + TABLE);
        sql("DROP DATABASE IF EXISTS fluss_catalog." + DB);

        try {
            admin.close();
        } catch (Exception e) {
            // ignore
        }
        try {
            connection.close();
        } catch (Exception e) {
            // ignore
        }
    }

    private static Map<String, String> getSparkConfigs(Configuration flussConf) {
        Map<String, String> configs = new HashMap<>();
        configs.put("spark.sql.catalog.fluss_catalog", SparkCatalog.class.getName());
        configs.put(
                "spark.sql.catalog.fluss_catalog.bootstrap.servers",
                String.join(",", flussConf.get(ConfigOptions.BOOTSTRAP_SERVERS)));
        return configs;
    }

    public static Dataset<Row> sql(String sqlText) {
        Dataset<Row> ds = spark.sql(sqlText);
        if (ds.columns().length == 0) {
            LOG.info("+----------------+");
            LOG.info("|  Empty Result  |");
            LOG.info("+----------------+");
        } else {
            ds.show(20, 50);
        }
        return ds;
    }
}
