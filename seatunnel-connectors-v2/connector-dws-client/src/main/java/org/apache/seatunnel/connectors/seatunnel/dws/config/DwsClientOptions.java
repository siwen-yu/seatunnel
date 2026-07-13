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

package org.apache.seatunnel.connectors.seatunnel.dws.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.sink.SchemaSaveMode;

import java.util.Collections;
import java.util.List;

/** Configuration options mapped directly to Huawei {@code DwsConfig}. */
public final class DwsClientOptions {
    public static final String CONNECTOR_IDENTITY = "DwsClient";

    public static final Option<String> URL =
            Options.key("url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "JDBC URL used by Huawei dws-client to connect to GaussDB(DWS).");
    public static final Option<String> USER =
            Options.key("user")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("GaussDB(DWS) user name.");
    public static final Option<String> PASSWORD =
            Options.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Password of the GaussDB(DWS) user.");
    public static final Option<String> TABLE =
            Options.key("table")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Target table name accepted by dws-client. Multi-table jobs can use placeholders such as ${schema_name}.${table_name}.");
    public static final Option<String> WRITE_MODE =
            Options.key("write_mode")
                    .stringType()
                    .defaultValue("AUTO")
                    .withDescription("Huawei dws-client write mode, for example AUTO or COPY.");
    public static final Option<String> CONFLICT_STRATEGY =
            Options.key("conflict_strategy")
                    .stringType()
                    .defaultValue("INSERT_OR_UPDATE")
                    .withDescription(
                            "Huawei dws-client strategy used when an inserted row conflicts with existing data.");
    public static final Option<Integer> BATCH_SIZE =
            Options.key("batch_size")
                    .intType()
                    .defaultValue(30000)
                    .withDescription(
                            "Number of buffered rows that triggers an automatic dws-client flush.");
    public static final Option<Long> FLUSH_INTERVAL_MS =
            Options.key("flush_interval_ms")
                    .longType()
                    .defaultValue(3000L)
                    .withDescription(
                            "Maximum interval in milliseconds between automatic dws-client flushes; 0 disables interval-based flushing.");
    public static final Option<Integer> WRITE_THREAD_SIZE =
            Options.key("write_thread_size")
                    .intType()
                    .defaultValue(1)
                    .withDescription(
                            "Number of dws-client write threads created in each SeaTunnel sink subtask.");
    public static final Option<Integer> COPY_WRITE_BATCH_SIZE =
            Options.key("copy_write_batch_size")
                    .intType()
                    .defaultValue(1000)
                    .withDescription("Number of rows in each dws-client COPY write batch.");
    public static final Option<Integer> MAX_RETRIES =
            Options.key("max_retries")
                    .intType()
                    .defaultValue(3)
                    .withDescription("Maximum number of retries for a failed dws-client flush.");
    public static final Option<Long> RETRY_BACKOFF_MS =
            Options.key("retry_backoff_ms")
                    .longType()
                    .defaultValue(1000L)
                    .withDescription("Base retry backoff in milliseconds used by dws-client.");
    public static final Option<Boolean> CASE_SENSITIVE =
            Options.key("case_sensitive")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether dws-client treats table and column names as case-sensitive.");
    public static final Option<List<String>> COMPARE_FIELDS =
            Options.key("compare_fields")
                    .listType()
                    .noDefaultValue()
                    .withDescription(
                            "Columns compared by dws-client to decide whether a conflicting row should update the existing row.");
    public static final Option<List<String>> PRIMARY_KEYS =
            Options.key("primary_keys")
                    .listType()
                    .noDefaultValue()
                    .withDescription(
                            "Target primary-key columns used by automatic table creation and dws-client conflict detection; the upstream primary key is used when omitted.");
    public static final Option<Boolean> ENABLE_DELETE =
            Options.key("enable_delete")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Whether DELETE changelog rows are submitted through dws-client; disabled mode rejects DELETE rows.");
    public static final Option<SchemaSaveMode> SCHEMA_SAVE_MODE =
            Options.key("schema_save_mode")
                    .enumType(SchemaSaveMode.class)
                    .defaultValue(SchemaSaveMode.CREATE_SCHEMA_WHEN_NOT_EXIST)
                    .withDescription(
                            "Controls target table creation: create when missing, recreate, require an existing table, or ignore schema handling.");

    private DwsClientOptions() {}

    public static OptionRule optionRule() {
        return OptionRule.builder()
                .required(URL, USER, PASSWORD, TABLE)
                .optional(
                        WRITE_MODE,
                        CONFLICT_STRATEGY,
                        BATCH_SIZE,
                        FLUSH_INTERVAL_MS,
                        WRITE_THREAD_SIZE,
                        COPY_WRITE_BATCH_SIZE,
                        MAX_RETRIES,
                        RETRY_BACKOFF_MS,
                        CASE_SENSITIVE,
                        COMPARE_FIELDS,
                        PRIMARY_KEYS,
                        ENABLE_DELETE,
                        SCHEMA_SAVE_MODE)
                .build();
    }

    public static DwsSinkConfig from(ReadonlyConfig config) {
        return new DwsSinkConfig(
                config.get(URL),
                config.get(USER),
                config.get(PASSWORD),
                config.get(TABLE),
                config.get(WRITE_MODE),
                config.get(CONFLICT_STRATEGY),
                config.get(BATCH_SIZE),
                config.get(FLUSH_INTERVAL_MS),
                config.get(WRITE_THREAD_SIZE),
                config.get(COPY_WRITE_BATCH_SIZE),
                config.get(MAX_RETRIES),
                config.get(RETRY_BACKOFF_MS),
                config.get(CASE_SENSITIVE),
                config.getOptional(COMPARE_FIELDS).orElse(Collections.emptyList()),
                config.getOptional(PRIMARY_KEYS).orElse(Collections.emptyList()),
                config.get(ENABLE_DELETE),
                config.get(SCHEMA_SAVE_MODE));
    }
}
