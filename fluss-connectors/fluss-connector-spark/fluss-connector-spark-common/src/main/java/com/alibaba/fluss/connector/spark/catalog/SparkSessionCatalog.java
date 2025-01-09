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

import com.alibaba.fluss.connector.spark.exception.CatalogException;
import com.alibaba.fluss.utils.Preconditions;

import org.apache.spark.sql.catalyst.analysis.NamespaceAlreadyExistsException;
import org.apache.spark.sql.catalyst.analysis.NoSuchDatabaseException;
import org.apache.spark.sql.catalyst.analysis.NoSuchFunctionException;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.NonEmptyNamespaceException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.connector.catalog.CatalogExtension;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.NamespaceChange;
import org.apache.spark.sql.connector.catalog.StagedTable;
import org.apache.spark.sql.connector.catalog.StagingTableCatalog;
import org.apache.spark.sql.connector.catalog.SupportsNamespaces;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.connector.catalog.functions.UnboundFunction;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.io.Closeable;
import java.util.Map;

/** A Spark Session Catalog for Fluss. */
public class SparkSessionCatalog<T extends TableCatalog & SupportsNamespaces>
        implements StagingTableCatalog, CatalogExtension, Closeable {

    private static final String[] DEFAULT_NAMESPACE = new String[] {"fluss"};

    private static final String FLUSS = "fluss";
    private static final String PROVIDER = "provider";

    private String catalogName;
    private SparkCatalog flussCatalog;
    private T sessionCatalog;

    @Override
    public void initialize(String name, CaseInsensitiveStringMap options) {
        Preconditions.checkArgument(
                "spark_catalog".equals(name), "catalog name must be set to spark_catalog");
        this.catalogName = name;
        this.flussCatalog = new SparkCatalog();
        this.flussCatalog.initialize(name, options);
    }

    @Override
    public String name() {
        return catalogName;
    }

    @Override
    public boolean functionExists(Identifier ident) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Identifier[] listFunctions(String[] strings) throws NoSuchNamespaceException {
        throw new UnsupportedOperationException();
    }

    @Override
    public UnboundFunction loadFunction(Identifier identifier) throws NoSuchFunctionException {
        throw new UnsupportedOperationException();
    }

    @Override
    public StagedTable stageCreate(
            Identifier identifier,
            StructType structType,
            Transform[] transforms,
            Map<String, String> map)
            throws TableAlreadyExistsException, NoSuchNamespaceException {
        throw new UnsupportedOperationException();
    }

    @Override
    public StagedTable stageReplace(
            Identifier identifier,
            StructType structType,
            Transform[] transforms,
            Map<String, String> map)
            throws NoSuchNamespaceException, NoSuchTableException {
        throw new UnsupportedOperationException();
    }

    @Override
    public StagedTable stageCreateOrReplace(
            Identifier identifier,
            StructType structType,
            Transform[] transforms,
            Map<String, String> map)
            throws NoSuchNamespaceException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean namespaceExists(String[] namespace) {
        return flussCatalog.namespaceExists(namespace)
                || getSessionCatalog().namespaceExists(namespace);
    }

    @Override
    public String[][] listNamespaces() throws NoSuchNamespaceException {
        String[][] flussNamespaces = flussCatalog.listNamespaces();
        String[][] sessionNamespaces = getSessionCatalog().listNamespaces();
        return mergeNamespaces(flussNamespaces, sessionNamespaces);
    }

    @Override
    public String[][] listNamespaces(String[] namespace) throws NoSuchNamespaceException {
        try {
            return flussCatalog.listNamespaces(namespace);
        } catch (NoSuchNamespaceException e) {
            return getSessionCatalog().listNamespaces(namespace);
        }
    }

    @Override
    public Map<String, String> loadNamespaceMetadata(String[] namespace)
            throws NoSuchNamespaceException {
        try {
            return flussCatalog.loadNamespaceMetadata(namespace);
        } catch (NoSuchNamespaceException e) {
            return getSessionCatalog().loadNamespaceMetadata(namespace);
        }
    }

    @Override
    public void createNamespace(String[] namespace, Map<String, String> metadata)
            throws NamespaceAlreadyExistsException {
        String provider = metadata.get(PROVIDER);
        if (useFluss(provider)) {
            flussCatalog.createNamespace(namespace, metadata);
        } else {
            getSessionCatalog().createNamespace(namespace, metadata);
        }
    }

    @Override
    public void alterNamespace(String[] namespace, NamespaceChange... namespaceChanges)
            throws NoSuchNamespaceException {
        // fluss catalog does not support alter namespace, so we use session catalog first,
        // if namespace not exists in session catalog, then we use fluss catalog
        try {
            getSessionCatalog().alterNamespace(namespace, namespaceChanges);
        } catch (NoSuchNamespaceException e) {
            flussCatalog.alterNamespace(namespace, namespaceChanges);
        }
    }

    @Override
    public boolean dropNamespace(String[] namespace, boolean cascade)
            throws NoSuchNamespaceException, NonEmptyNamespaceException {
        return flussCatalog.dropNamespace(namespace, cascade)
                || getSessionCatalog().dropNamespace(namespace, cascade);
    }

    @Override
    public void setDelegateCatalog(CatalogPlugin catalogPlugin) {
        if (catalogPlugin instanceof TableCatalog && catalogPlugin instanceof SupportsNamespaces) {
            this.sessionCatalog = (T) catalogPlugin;
        } else {
            throw new IllegalArgumentException("Invalid session catalog: " + catalogPlugin);
        }
    }

    @Override
    public String[] defaultNamespace() {
        return DEFAULT_NAMESPACE;
    }

    @Override
    public Table loadTable(Identifier ident, String version) throws NoSuchTableException {
        // fluss catalog does not support load table with version, so we use session catalog first,
        // if table not exists in session catalog, then we use fluss catalog
        try {
            return getSessionCatalog().loadTable(ident, version);
        } catch (NoSuchTableException e) {
            return flussCatalog.loadTable(ident, version);
        }
    }

    @Override
    public Table loadTable(Identifier ident, long timestamp) throws NoSuchTableException {
        // fluss catalog does not support load table with timestamp, so we use session catalog
        // first, if table not exists in session catalog, then we use fluss catalog
        try {
            return getSessionCatalog().loadTable(ident, timestamp);
        } catch (NoSuchTableException e) {
            return flussCatalog.loadTable(ident, timestamp);
        }
    }

    @Override
    public void invalidateTable(Identifier ident) {
        getSessionCatalog().invalidateTable(ident);
        flussCatalog.invalidateTable(ident);
    }

    @Override
    public boolean tableExists(Identifier ident) {
        boolean existsInFluss = flussCatalog.tableExists(ident);
        if (!existsInFluss) {
            try {
                // session catalog use loadTable method to check if table exists,
                // if database not exists, which will throw NoSuchDatabaseException
                return getSessionCatalog().tableExists(ident);
            } catch (Exception e) {
                if (e instanceof NoSuchDatabaseException) {
                    return false;
                }
                throw new CatalogException(
                        String.format(
                                "Failed to judge table %s exists by catalog %s",
                                ident.toString(), name()),
                        e);
            }
        }
        return true;
    }

    @Override
    public boolean purgeTable(Identifier ident) throws UnsupportedOperationException {
        // fluss catalog does not support purge table, so we use session catalog first,
        // if return false in session catalog, then we use fluss catalog
        return getSessionCatalog().purgeTable(ident) || flussCatalog.purgeTable(ident);
    }

    @Override
    public Identifier[] listTables(String[] namespace) throws NoSuchNamespaceException {
        try {
            return flussCatalog.listTables(namespace);
        } catch (NoSuchNamespaceException e) {
            return getSessionCatalog().listTables(namespace);
        }
    }

    @Override
    public Table loadTable(Identifier ident) throws NoSuchTableException {
        try {
            return flussCatalog.loadTable(ident);
        } catch (NoSuchTableException e) {
            return getSessionCatalog().loadTable(ident);
        }
    }

    @Override
    public Table createTable(
            Identifier ident,
            StructType structType,
            Transform[] transforms,
            Map<String, String> properties)
            throws TableAlreadyExistsException, NoSuchNamespaceException {
        String provider = properties.get(PROVIDER);
        if (useFluss(provider)) {
            return flussCatalog.createTable(ident, structType, transforms, properties);
        } else {
            // delegate to the session catalog
            return getSessionCatalog().createTable(ident, structType, transforms, properties);
        }
    }

    @Override
    public Table alterTable(Identifier ident, TableChange... tableChanges)
            throws NoSuchTableException {
        // fluss catalog does not support alter table, so we use session catalog first,
        // if table not exists in session catalog, then we use fluss catalog
        try {
            return getSessionCatalog().alterTable(ident, tableChanges);
        } catch (NoSuchTableException e) {
            return flussCatalog.alterTable(ident, tableChanges);
        }
    }

    @Override
    public boolean dropTable(Identifier identifier) {
        return flussCatalog.dropTable(identifier) || getSessionCatalog().dropTable(identifier);
    }

    @Override
    public void renameTable(Identifier oldIdent, Identifier newIdent)
            throws NoSuchTableException, TableAlreadyExistsException {
        // fluss catalog does not support rename table, so we use session catalog first,
        // if table not exists in session catalog, then we use fluss catalog
        try {
            getSessionCatalog().renameTable(oldIdent, newIdent);
        } catch (NoSuchTableException e) {
            flussCatalog.renameTable(oldIdent, newIdent);
        }
    }

    @Override
    public void close() {
        if (flussCatalog != null) {
            flussCatalog.close();
        }
    }

    private String[][] mergeNamespaces(String[][] flussNamespaces, String[][] sessionNamespaces) {
        if (flussNamespaces == null && sessionNamespaces == null) {
            return new String[0][];
        } else if (flussNamespaces == null && sessionNamespaces != null) {
            return sessionNamespaces;
        } else if (flussNamespaces != null && sessionNamespaces == null) {
            return flussNamespaces;
        }
        String[][] namespaces = new String[flussNamespaces.length + sessionNamespaces.length][];
        System.arraycopy(flussNamespaces, 0, namespaces, 0, flussNamespaces.length);
        System.arraycopy(
                sessionNamespaces, 0, namespaces, flussNamespaces.length, sessionNamespaces.length);
        return namespaces;
    }

    private T getSessionCatalog() {
        Preconditions.checkNotNull(
                sessionCatalog,
                "Delegated SessionCatalog is missing. "
                        + "Please make sure your are replacing Spark's default catalog, named 'spark_catalog'.");
        return sessionCatalog;
    }

    private boolean useFluss(String provider) {
        return provider == null || FLUSS.equalsIgnoreCase(provider);
    }
}
