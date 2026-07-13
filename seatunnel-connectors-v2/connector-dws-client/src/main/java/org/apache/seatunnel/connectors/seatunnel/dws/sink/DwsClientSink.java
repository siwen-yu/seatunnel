/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.dws.sink;

import org.apache.seatunnel.api.sink.DataSaveMode;
import org.apache.seatunnel.api.sink.SaveModeHandler;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportMultiTableSink;
import org.apache.seatunnel.api.sink.SupportSaveMode;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;
import org.apache.seatunnel.connectors.seatunnel.dws.catalog.DwsCatalog;
import org.apache.seatunnel.connectors.seatunnel.dws.config.DwsClientOptions;
import org.apache.seatunnel.connectors.seatunnel.dws.config.DwsSinkConfig;

import com.huaweicloud.dws.client.model.TableName;

import java.io.IOException;
import java.util.Optional;

final class DwsClientSink
        implements SeaTunnelSink<SeaTunnelRow, Void, Void, Void>,
                SupportMultiTableSink,
                SupportSaveMode {
    private static final long serialVersionUID = 1L;

    private final DwsSinkConfig config;
    private final CatalogTable catalogTable;

    DwsClientSink(DwsSinkConfig config, CatalogTable catalogTable) {
        this.config = config;
        this.catalogTable = catalogTable;
    }

    @Override
    public String getPluginName() {
        return DwsClientOptions.CONNECTOR_IDENTITY;
    }

    @Override
    public SinkWriter<SeaTunnelRow, Void, Void> createWriter(SinkWriter.Context context)
            throws IOException {
        try {
            return new DwsClientSinkWriter(
                    config,
                    catalogTable.getSeaTunnelRowType(),
                    new HuaweiDwsClientFacade(config, context.getIndexOfSubtask()));
        } catch (Exception e) {
            throw new IOException("Unable to create Huawei dws-client", e);
        }
    }

    @Override
    public Optional<CatalogTable> getWriteCatalogTable() {
        // MultiTableSink uses this metadata to retain the physical table identity after wrapping
        // all per-table DWS sinks into one runtime sink.
        return Optional.of(catalogTable);
    }

    @Override
    public Optional<SaveModeHandler> getSaveModeHandler() {
        TableName target = TableName.valueOf(config.getTable());
        // AbstractJdbcCatalog resolves connections per database, so the database selected by the
        // JDBC URL must be part of the table path; the schema defaults to public like dws-client.
        String database =
                JdbcUrlUtil.getUrlInfo(config.getUrl())
                        .getDefaultDatabase()
                        .orElseThrow(
                                () ->
                                        new IllegalArgumentException(
                                                "The DWS JDBC url must contain a database: "
                                                        + config.getUrl()));
        String schema = target.getSchemaName() == null ? "public" : target.getSchemaName();
        TablePath tablePath = TablePath.of(database, schema, target.getTableName());
        DwsCatalog catalog =
                new DwsCatalog(
                        config.getUrl(),
                        config.getUser(),
                        config.getPassword(),
                        config.getPrimaryKeys());
        if (config.getPrimaryKeys().isEmpty()) {
            // dws-client appends or merges rows itself; SaveMode only owns target schema lifecycle.
            return Optional.of(
                    new DwsSaveModeHandler(
                            config.getSchemaSaveMode(),
                            DataSaveMode.APPEND_DATA,
                            catalog,
                            tablePath,
                            catalogTable));
        } else {
            // dws-client appends or merges rows itself; SaveMode only owns target schema lifecycle.
            TableSchema tableSchema = catalogTable.getTableSchema();
            PrimaryKey configPk =
                    PrimaryKey.of(
                            catalogTable.getTablePath().getTableName() + "_config_pk",
                            config.getPrimaryKeys());
            TableSchema configTableSchema =
                    new TableSchema(
                            tableSchema.getColumns(), configPk, tableSchema.getConstraintKeys());
            CatalogTable.of(
                    catalogTable.getTableId(),
                    configTableSchema,
                    catalogTable.getOptions(),
                    catalogTable.getPartitionKeys(),
                    catalogTable.getComment());
            return Optional.of(
                    new DwsSaveModeHandler(
                            config.getSchemaSaveMode(),
                            DataSaveMode.APPEND_DATA,
                            catalog,
                            tablePath,
                            catalogTable));
        }
    }
}
