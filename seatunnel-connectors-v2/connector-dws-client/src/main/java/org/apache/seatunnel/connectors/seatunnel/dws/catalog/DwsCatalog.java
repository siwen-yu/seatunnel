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

package org.apache.seatunnel.connectors.seatunnel.dws.catalog;

import org.apache.seatunnel.api.table.catalog.exception.CatalogException;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.psql.PostgresCatalog;

import java.util.List;

/** GaussDB(DWS) catalog that reuses {@link PostgresCatalog} for all metadata operations and only */
public class DwsCatalog extends PostgresCatalog {
    private static final String CATALOG_NAME = "DwsClient";
    private static final String DRIVER_NAME = "com.huawei.gauss200.jdbc.Driver";
    private static final String POSTGRESQL_DRIVER_NAME = "org.postgresql.Driver";

    public DwsCatalog(String url, String user, String password, List<String> primaryKeys) {
        super(CATALOG_NAME, user, password, JdbcUrlUtil.getUrlInfo(url), null, DRIVER_NAME);
    }

    @Override
    public void open() throws CatalogException {
        try {
            // The GaussDB(DWS) driver may not be discoverable via SPI; register it explicitly
            // before the shared JDBC catalog resolves the connection.
            Class.forName(DRIVER_NAME);
            Class.forName(POSTGRESQL_DRIVER_NAME);
        } catch (ClassNotFoundException e) {
            throw new CatalogException("Unable to load the GaussDB(DWS) JDBC driver", e);
        }
        super.open();
    }
}
