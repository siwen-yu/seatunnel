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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public final class DwsSinkConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String url;
    private final String user;
    private final String password;
    private final String table;
    private final String writeMode;
    private final String conflictStrategy;
    private final int batchSize;
    private final long flushIntervalMs;
    private final int writeThreadSize;
    private final int copyWriteBatchSize;
    private final int maxRetries;
    private final long retryBackoffMs;
    private final boolean caseSensitive;
    private final List<String> compareFields;
    private final boolean enableDelete;

    public DwsSinkConfig(
            String url,
            String user,
            String password,
            String table,
            String writeMode,
            String conflictStrategy,
            int batchSize,
            long flushIntervalMs,
            int writeThreadSize,
            int copyWriteBatchSize,
            int maxRetries,
            long retryBackoffMs,
            boolean caseSensitive,
            List<String> compareFields,
            boolean enableDelete) {
        if (batchSize <= 0
                || flushIntervalMs < 0
                || writeThreadSize <= 0
                || copyWriteBatchSize <= 0
                || maxRetries < 0
                || retryBackoffMs < 0) {
            throw new IllegalArgumentException("Invalid batch/retry configuration");
        }
        this.url = require(url, "url");
        this.user = require(user, "user");
        this.password = require(password, "password");
        this.table = require(table, "table");
        this.writeMode = require(writeMode, "write_mode");
        this.conflictStrategy = require(conflictStrategy, "conflict_strategy");
        this.batchSize = batchSize;
        this.flushIntervalMs = flushIntervalMs;
        this.writeThreadSize = writeThreadSize;
        this.copyWriteBatchSize = copyWriteBatchSize;
        this.maxRetries = maxRetries;
        this.retryBackoffMs = retryBackoffMs;
        this.caseSensitive = caseSensitive;
        this.compareFields = Collections.unmodifiableList(new ArrayList<>(compareFields));
        this.enableDelete = enableDelete;
    }

    private static String require(String value, String name) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value;
    }

    public String getUrl() {
        return url;
    }

    public String getUser() {
        return user;
    }

    public String getPassword() {
        return password;
    }

    public String getTable() {
        return table;
    }

    public String getWriteMode() {
        return writeMode;
    }

    public String getConflictStrategy() {
        return conflictStrategy;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public long getFlushIntervalMs() {
        return flushIntervalMs;
    }

    public int getWriteThreadSize() {
        return writeThreadSize;
    }

    public int getCopyWriteBatchSize() {
        return copyWriteBatchSize;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public long getRetryBackoffMs() {
        return retryBackoffMs;
    }

    public boolean isCaseSensitive() {
        return caseSensitive;
    }

    public List<String> getCompareFields() {
        return compareFields;
    }

    public boolean isEnableDelete() {
        return enableDelete;
    }
}
