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

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DwsCreateTableSqlBuilderTest {

    @Test
    void mapsSupportedScalarTypes() {
        TableSchema schema =
                TableSchema.builder()
                        .column(column("bool_col", BasicType.BOOLEAN_TYPE, null))
                        .column(column("tiny_col", BasicType.BYTE_TYPE, null))
                        .column(column("small_col", BasicType.SHORT_TYPE, null))
                        .column(column("int_col", BasicType.INT_TYPE, null))
                        .column(column("big_col", BasicType.LONG_TYPE, null))
                        .column(column("float_col", BasicType.FLOAT_TYPE, null))
                        .column(column("double_col", BasicType.DOUBLE_TYPE, null))
                        .column(column("decimal_col", new DecimalType(20, 4), null))
                        .column(column("varchar_col", BasicType.STRING_TYPE, 128L))
                        .column(column("text_col", BasicType.STRING_TYPE, null))
                        .column(column("bytes_col", PrimitiveByteArrayType.INSTANCE, null))
                        .column(column("date_col", LocalTimeType.LOCAL_DATE_TYPE, null))
                        .column(column("time_col", LocalTimeType.LOCAL_TIME_TYPE, null))
                        .column(column("timestamp_col", LocalTimeType.LOCAL_DATE_TIME_TYPE, null))
                        .column(
                                column(
                                        "timestamp_tz_col",
                                        LocalTimeType.OFFSET_DATE_TIME_TYPE,
                                        null))
                        .build();

        String sql = build(schema, Collections.emptyList());

        assertTrue(sql.contains("\"bool_col\" BOOLEAN"));
        assertTrue(sql.contains("\"tiny_col\" SMALLINT"));
        assertTrue(sql.contains("\"small_col\" SMALLINT"));
        assertTrue(sql.contains("\"int_col\" INTEGER"));
        assertTrue(sql.contains("\"big_col\" BIGINT"));
        assertTrue(sql.contains("\"float_col\" REAL"));
        assertTrue(sql.contains("\"double_col\" DOUBLE PRECISION"));
        assertTrue(sql.contains("\"decimal_col\" DECIMAL(20,4)"));
        assertTrue(sql.contains("\"varchar_col\" VARCHAR(128)"));
        assertTrue(sql.contains("\"text_col\" TEXT"));
        assertTrue(sql.contains("\"bytes_col\" BYTEA"));
        assertTrue(sql.contains("\"date_col\" DATE"));
        assertTrue(sql.contains("\"time_col\" TIME"));
        assertTrue(sql.contains("\"timestamp_col\" TIMESTAMP"));
        assertTrue(sql.contains("\"timestamp_tz_col\" TIMESTAMP WITH TIME ZONE"));
        assertTrue(sql.endsWith("DISTRIBUTE BY ROUNDROBIN"));
    }

    @Test
    void configuredPrimaryKeysOverrideUpstreamKeyAndDriveHashDistribution() {
        TableSchema schema =
                TableSchema.builder()
                        .column(column("tenant_id", BasicType.INT_TYPE, null))
                        .column(column("id", BasicType.LONG_TYPE, null))
                        .column(column("name", BasicType.STRING_TYPE, null))
                        .primaryKey(PrimaryKey.of("source_pk", Collections.singletonList("id")))
                        .build();

        String sql = build(schema, Arrays.asList("tenant_id", "id"));

        assertTrue(sql.contains("\"tenant_id\" INTEGER NOT NULL"));
        assertTrue(sql.contains("PRIMARY KEY (\"tenant_id\", \"id\")"));
        assertTrue(sql.endsWith("DISTRIBUTE BY HASH (\"tenant_id\", \"id\")"));
    }

    @Test
    void usesUpstreamPrimaryKeyWhenNoKeyIsConfigured() {
        TableSchema schema =
                TableSchema.builder()
                        .column(column("id", BasicType.LONG_TYPE, null))
                        .column(column("name", BasicType.STRING_TYPE, null))
                        .primaryKey(PrimaryKey.of("source_pk", Collections.singletonList("id")))
                        .build();

        String sql = build(schema, Collections.emptyList());

        assertTrue(sql.contains("PRIMARY KEY (\"id\")"));
        assertTrue(sql.endsWith("DISTRIBUTE BY HASH (\"id\")"));
    }

    @Test
    void rejectsUnsupportedComplexTypes() {
        TableSchema schema =
                TableSchema.builder()
                        .column(column("tags", ArrayType.STRING_ARRAY_TYPE, null))
                        .build();

        IllegalArgumentException error =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> build(schema, Collections.emptyList()));

        assertTrue(error.getMessage().contains("ARRAY"));
        assertTrue(error.getMessage().contains("tags"));
    }

    @Test
    void rejectsMissingCompareField() {
        TableSchema schema =
                TableSchema.builder().column(column("id", BasicType.INT_TYPE, null)).build();

        IllegalArgumentException error =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> build(schema, Collections.singletonList("missing")));

        assertTrue(error.getMessage().contains("missing"));
    }

    private static String build(TableSchema schema, java.util.List<String> primaryKeys) {
        CatalogTable table =
                CatalogTable.of(
                        TableIdentifier.of("catalog", "database", "ods", "target"),
                        schema,
                        Collections.emptyMap(),
                        Collections.emptyList(),
                        "DWS create table test");
        return DwsCreateTableSqlBuilder.build(
                TablePath.of(null, "ods", "target"), table, primaryKeys);
    }

    private static PhysicalColumn column(String name, SeaTunnelDataType<?> dataType, Long length) {
        return PhysicalColumn.builder()
                .name(name)
                .dataType(dataType)
                .columnLength(length)
                .nullable(true)
                .build();
    }
}
