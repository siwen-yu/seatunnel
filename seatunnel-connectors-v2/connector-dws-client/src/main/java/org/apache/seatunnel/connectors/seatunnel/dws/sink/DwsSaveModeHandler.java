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
import org.apache.seatunnel.api.sink.DefaultSaveModeHandler;
import org.apache.seatunnel.api.sink.SchemaSaveMode;
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;

/** Save-mode handler that treats a GaussDB(DWS) schema as a schema, not a database. */
final class DwsSaveModeHandler extends DefaultSaveModeHandler {

    DwsSaveModeHandler(
            SchemaSaveMode schemaSaveMode,
            DataSaveMode dataSaveMode,
            Catalog catalog,
            TablePath tablePath,
            CatalogTable catalogTable) {
        super(schemaSaveMode, dataSaveMode, catalog, tablePath, catalogTable, null);
    }

    @Override
    protected void createTable() {
        // DefaultSaveModeHandler attempts CREATE DATABASE first. The schema in a DWS table name
        // already belongs to the database selected by the JDBC URL and must not trigger that path.
        catalog.createTable(tablePath, catalogTable, true);
    }
}
