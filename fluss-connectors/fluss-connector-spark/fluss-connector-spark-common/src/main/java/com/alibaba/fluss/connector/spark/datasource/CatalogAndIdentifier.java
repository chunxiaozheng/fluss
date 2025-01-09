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

import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;

/** A class which contains a TableCatalog and a Identifier. */
public class CatalogAndIdentifier {
    private final TableCatalog tableCatalog;
    private final Identifier identifier;

    public CatalogAndIdentifier(CatalogPlugin catalog, Identifier identifier) {
        Preconditions.checkArgument(
                catalog instanceof TableCatalog, "catalog must be a TableCatalog");
        this.tableCatalog = (TableCatalog) catalog;
        this.identifier = identifier;
    }

    public TableCatalog catalog() {
        return this.tableCatalog;
    }

    public Identifier identifier() {
        return this.identifier;
    }
}
