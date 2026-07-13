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

import java.util.Collections;
import java.util.List;

public final class DwsClientOptions {
    public static final String CONNECTOR_IDENTITY = "DwsClient";

    public static final Option<String> URL = Options.key("url").stringType().noDefaultValue();
    public static final Option<String> USER = Options.key("user").stringType().noDefaultValue();
    public static final Option<String> PASSWORD =
            Options.key("password").stringType().noDefaultValue();
    public static final Option<String> TABLE = Options.key("table").stringType().noDefaultValue();
    public static final Option<String> WRITE_MODE =
            Options.key("write_mode").stringType().defaultValue("AUTO");
    public static final Option<String> CONFLICT_STRATEGY =
            Options.key("conflict_strategy").stringType().defaultValue("INSERT_OR_UPDATE");
    public static final Option<Integer> BATCH_SIZE =
            Options.key("batch_size").intType().defaultValue(30000);
    public static final Option<Long> FLUSH_INTERVAL_MS =
            Options.key("flush_interval_ms").longType().defaultValue(3000L);
    public static final Option<Integer> WRITE_THREAD_SIZE =
            Options.key("write_thread_size").intType().defaultValue(1);
    public static final Option<Integer> COPY_WRITE_BATCH_SIZE =
            Options.key("copy_write_batch_size").intType().defaultValue(1000);
    public static final Option<Integer> MAX_RETRIES =
            Options.key("max_retries").intType().defaultValue(3);
    public static final Option<Long> RETRY_BACKOFF_MS =
            Options.key("retry_backoff_ms").longType().defaultValue(1000L);
    public static final Option<Boolean> CASE_SENSITIVE =
            Options.key("case_sensitive").booleanType().defaultValue(false);
    public static final Option<List<String>> COMPARE_FIELDS =
            Options.key("compare_fields").listType().noDefaultValue();
    public static final Option<Boolean> ENABLE_DELETE =
            Options.key("enable_delete").booleanType().defaultValue(true);

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
                        ENABLE_DELETE)
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
                config.get(ENABLE_DELETE));
    }
}
