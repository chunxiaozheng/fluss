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

package com.alibaba.fluss.connector.spark.datasource;

import com.alibaba.fluss.utils.Preconditions;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.connector.catalog.CatalogManager;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.SupportsCatalogOptions;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/** A fluss data source of Spark. */
public class FlussDataSource implements DataSourceRegister, SupportsCatalogOptions {

    @Override
    public String shortName() {
        return "fluss";
    }

    @Override
    public String extractCatalog(CaseInsensitiveStringMap options) {
        return catalogAndIdentifier(options).catalog().name();
    }

    @Override
    public Identifier extractIdentifier(CaseInsensitiveStringMap options) {
        return catalogAndIdentifier(options).identifier();
    }

    @Override
    public boolean supportsExternalMetadata() {
        return true;
    }

    @Override
    public StructType inferSchema(CaseInsensitiveStringMap caseInsensitiveStringMap) {
        return null;
    }

    @Override
    public Table getTable(
            StructType structType, Transform[] transforms, Map<String, String> options) {
        CatalogAndIdentifier catalogAndIdentifier =
                catalogAndIdentifier(new CaseInsensitiveStringMap(options));
        TableCatalog catalog = catalogAndIdentifier.catalog();
        Identifier ident = catalogAndIdentifier.identifier();

        try {
            return catalog.loadTable(ident);
        } catch (NoSuchTableException e) {
            throw new UnsupportedOperationException(String.format("%s not exists", ident));
        }
    }

    private CatalogAndIdentifier catalogAndIdentifier(CaseInsensitiveStringMap options) {
        Preconditions.checkArgument(
                options.containsKey("path"), "Cannot open table: path is not set");
        String path = options.get("path");
        Preconditions.checkArgument(
                !path.contains("/"), "invalid table identifier %s, contain '/'", path);
        List<String> nameParts = Arrays.asList(path.split("\\."));
        SparkSession spark = SparkSession.active();
        CatalogManager catalogManager = spark.sessionState().catalogManager();
        CatalogPlugin currentCatalog = catalogManager.currentCatalog();
        String[] currentNamespace = catalogManager.currentNamespace();

        int lastElementIndex = nameParts.size() - 1;
        String name = nameParts.get(lastElementIndex);

        if (nameParts.size() == 1) {
            // Only a single element, use current catalog and namespace
            return new CatalogAndIdentifier(currentCatalog, Identifier.of(currentNamespace, name));
        } else {
            CatalogPlugin catalog = catalogManager.catalog(nameParts.get(0));
            if (catalog == null) {
                // The first element was not a valid catalog, treat it like part of the namespace
                String[] namespace = nameParts.subList(0, lastElementIndex).toArray(new String[0]);
                return new CatalogAndIdentifier(currentCatalog, Identifier.of(namespace, name));
            } else {
                // Assume the first element is a valid catalog
                String[] namespace = nameParts.subList(1, lastElementIndex).toArray(new String[0]);
                return new CatalogAndIdentifier(catalog, Identifier.of(namespace, name));
            }
        }
    }
}
