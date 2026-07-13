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
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/** Builds GaussDB(DWS) column-store DDL from SeaTunnel catalog metadata. */
public final class DwsCreateTableSqlBuilder {
    private static final long MAX_VARCHAR_LENGTH = 10_485_760L;

    private DwsCreateTableSqlBuilder() {}

    public static String build(
            TablePath tablePath, CatalogTable catalogTable, List<String> configuredPrimaryKeys) {
        List<Column> columns =
                catalogTable.getTableSchema().getColumns().stream()
                        .filter(Column::isPhysical)
                        .collect(Collectors.toList());
        if (columns.isEmpty()) {
            throw new IllegalArgumentException(
                    "Cannot create DWS table without physical columns: "
                            + tablePath.getSchemaAndTableName());
        }

        List<String> keyColumns = resolveKeyColumns(catalogTable, configuredPrimaryKeys);
        validateKeyColumns(columns, keyColumns);
        Set<String> keyColumnSet = new LinkedHashSet<>(keyColumns);

        List<String> definitions = new ArrayList<>();
        for (Column column : columns) {
            boolean nullable = column.isNullable() && !keyColumnSet.contains(column.getName());
            definitions.add(
                    "    "
                            + quoteIdentifier(column.getName())
                            + " "
                            + toDwsType(column)
                            + (nullable ? "" : " NOT NULL"));
        }
        if (!keyColumns.isEmpty()) {
            definitions.add("    PRIMARY KEY (" + quoteIdentifiers(keyColumns) + ")");
        }

        String distribution =
                keyColumns.isEmpty()
                        ? "DISTRIBUTE BY ROUNDROBIN"
                        : "DISTRIBUTE BY HASH (" + quoteIdentifiers(keyColumns) + ")";
        return "CREATE TABLE "
                + qualifiedName(tablePath)
                + " (\n"
                + String.join(",\n", definitions)
                + "\n)\nWITH (orientation=row, compression=no)\n"
                + distribution;
    }

    private static List<String> resolveKeyColumns(
            CatalogTable catalogTable, List<String> configuredPrimaryKeys) {
        if (configuredPrimaryKeys != null && !configuredPrimaryKeys.isEmpty()) {
            return new ArrayList<>(new LinkedHashSet<>(configuredPrimaryKeys));
        }
        PrimaryKey primaryKey = catalogTable.getTableSchema().getPrimaryKey();
        if (primaryKey == null || primaryKey.getColumnNames() == null) {
            return Collections.emptyList();
        }
        return new ArrayList<>(new LinkedHashSet<>(primaryKey.getColumnNames()));
    }

    private static void validateKeyColumns(List<Column> columns, List<String> keyColumns) {
        Set<String> columnNames = columns.stream().map(Column::getName).collect(Collectors.toSet());
        for (String keyColumn : keyColumns) {
            if (!columnNames.contains(keyColumn)) {
                throw new IllegalArgumentException(
                        "DWS key column '" + keyColumn + "' does not exist in the sink schema");
            }
        }
    }

    private static String toDwsType(Column column) {
        SeaTunnelDataType<?> dataType = column.getDataType();
        switch (dataType.getSqlType()) {
            case BOOLEAN:
                return "BOOLEAN";
            case TINYINT:
            case SMALLINT:
                return "SMALLINT";
            case INT:
                return "INTEGER";
            case BIGINT:
                return "BIGINT";
            case FLOAT:
                return "REAL";
            case DOUBLE:
                return "DOUBLE PRECISION";
            case DECIMAL:
                DecimalType decimalType = (DecimalType) dataType;
                return "DECIMAL(" + decimalType.getPrecision() + "," + decimalType.getScale() + ")";
            case STRING:
                Long length = column.getColumnLength();
                return length != null && length > 0 && length <= MAX_VARCHAR_LENGTH
                        ? "VARCHAR(" + length + ")"
                        : "TEXT";
            case BYTES:
                return "BYTEA";
            case DATE:
                return "DATE";
            case TIME:
                return "TIME";
            case TIMESTAMP:
                return "TIMESTAMP";
            case TIMESTAMP_TZ:
                return "TIMESTAMP WITH TIME ZONE";
            default:
                throw new IllegalArgumentException(
                        "SeaTunnel type "
                                + dataType.getSqlType()
                                + " is not supported by DWS automatic table creation for column '"
                                + column.getName()
                                + "'");
        }
    }

    static String qualifiedName(TablePath tablePath) {
        String tableName = quoteIdentifier(tablePath.getTableName());
        return tablePath.getSchemaName() == null
                ? tableName
                : quoteIdentifier(tablePath.getSchemaName()) + "." + tableName;
    }

    static String quoteIdentifier(String identifier) {
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }

    private static String quoteIdentifiers(List<String> identifiers) {
        return identifiers.stream()
                .map(DwsCreateTableSqlBuilder::quoteIdentifier)
                .collect(Collectors.joining(", "));
    }
}
