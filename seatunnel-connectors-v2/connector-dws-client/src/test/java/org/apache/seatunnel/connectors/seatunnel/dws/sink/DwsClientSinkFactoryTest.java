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

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.SchemaSaveMode;
import org.apache.seatunnel.api.sink.SupportMultiTableSink;
import org.apache.seatunnel.api.sink.SupportSaveMode;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.connectors.seatunnel.dws.config.DwsClientOptions;
import org.apache.seatunnel.connectors.seatunnel.dws.config.DwsSinkConfig;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DwsClientSinkFactoryTest {
    @Test
    void factoryIsDiscoverableThroughSeaTunnelSpi() {
        boolean found = false;
        for (Factory factory : ServiceLoader.load(Factory.class)) {
            if (factory instanceof DwsClientSinkFactory) {
                found = true;
                assertEquals("DwsClient", factory.factoryIdentifier());
            }
        }
        assertTrue(found, "DwsClientSinkFactory was not registered as a SeaTunnel Factory");
    }

    @Test
    void readsRequiredOptionsAndOfficialClientDefaults() {
        Map<String, Object> values = new HashMap<>();
        values.put("url", "jdbc:postgresql://localhost:8000/db");
        values.put("user", "user");
        values.put("password", "password");
        values.put("table", "public.target");
        values.put("primary_keys", Collections.singletonList("id"));

        DwsSinkConfig config = DwsClientOptions.from(ReadonlyConfig.fromMap(values));

        assertEquals("AUTO", config.getWriteMode());
        assertEquals("INSERT_OR_UPDATE", config.getConflictStrategy());
        assertEquals(30000, config.getBatchSize());
        assertEquals(3000, config.getFlushIntervalMs());
        assertTrue(config.isEnableDelete());
        assertEquals(Collections.singletonList("id"), config.getPrimaryKeys());
        assertEquals(SchemaSaveMode.CREATE_SCHEMA_WHEN_NOT_EXIST, config.getSchemaSaveMode());
    }

    @Test
    void allOptionsHaveDescriptions() throws IllegalAccessException {
        int optionCount = 0;
        for (Field field : DwsClientOptions.class.getDeclaredFields()) {
            if (Modifier.isStatic(field.getModifiers())
                    && Option.class.isAssignableFrom(field.getType())) {
                Option<?> option = (Option<?>) field.get(null);
                assertTrue(
                        option.getDescription() != null
                                && !option.getDescription().trim().isEmpty(),
                        "Missing description for option: " + option.key());
                optionCount++;
            }
        }
        assertEquals(17, optionCount);
    }

    @Test
    void createsMultiTableSinkWithResolvedTargetTable() {
        CatalogTable orders = catalogTable("public", "orders");
        CatalogTable customers = catalogTable("audit", "customers");
        Map<String, Object> values = requiredOptions("${schema_name}.${table_name}");
        TableSinkFactoryContext ordersContext =
                TableSinkFactoryContext.replacePlaceholderAndCreate(
                        orders,
                        ReadonlyConfig.fromMap(values),
                        getClass().getClassLoader(),
                        Collections.emptySet());
        TableSinkFactoryContext customersContext =
                TableSinkFactoryContext.replacePlaceholderAndCreate(
                        customers,
                        ReadonlyConfig.fromMap(values),
                        getClass().getClassLoader(),
                        Collections.emptySet());

        DwsSinkConfig ordersConfig = DwsClientOptions.from(ordersContext.getOptions());
        DwsSinkConfig customersConfig = DwsClientOptions.from(customersContext.getOptions());
        DwsClientSink ordersSink =
                (DwsClientSink) new DwsClientSinkFactory().createSink(ordersContext).createSink();
        DwsClientSink customersSink =
                (DwsClientSink)
                        new DwsClientSinkFactory().createSink(customersContext).createSink();

        assertEquals("public.orders", ordersConfig.getTable());
        assertEquals("audit.customers", customersConfig.getTable());
        assertTrue(ordersSink instanceof SupportMultiTableSink);
        assertTrue(customersSink instanceof SupportMultiTableSink);
        assertTrue(ordersSink instanceof SupportSaveMode);
        assertTrue(customersSink instanceof SupportSaveMode);
        assertEquals(orders, ordersSink.getWriteCatalogTable().orElse(null));
        assertEquals(customers, customersSink.getWriteCatalogTable().orElse(null));
        assertEquals(
                "public.orders",
                ordersSink
                        .getSaveModeHandler()
                        .orElseThrow(AssertionError::new)
                        .getHandleTablePath()
                        .getSchemaAndTableName());
        assertEquals(
                "audit.customers",
                customersSink
                        .getSaveModeHandler()
                        .orElseThrow(AssertionError::new)
                        .getHandleTablePath()
                        .getSchemaAndTableName());
    }

    private static Map<String, Object> requiredOptions(String table) {
        Map<String, Object> values = new HashMap<>();
        values.put("url", "jdbc:postgresql://localhost:8000/db");
        values.put("user", "user");
        values.put("password", "password");
        values.put("table", table);
        return values;
    }

    private static CatalogTable catalogTable(String schema, String table) {
        TableSchema tableSchema =
                TableSchema.builder()
                        .column(
                                PhysicalColumn.builder()
                                        .name("id")
                                        .dataType(BasicType.INT_TYPE)
                                        .nullable(false)
                                        .build())
                        .build();
        return CatalogTable.of(
                TableIdentifier.of("catalog", "database", schema, table),
                tableSchema,
                Collections.emptyMap(),
                Collections.emptyList(),
                "DWS multi-table sink test");
    }
}
