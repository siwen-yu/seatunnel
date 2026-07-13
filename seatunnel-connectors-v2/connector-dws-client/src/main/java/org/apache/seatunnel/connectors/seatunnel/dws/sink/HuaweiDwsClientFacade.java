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

import org.apache.seatunnel.connectors.seatunnel.dws.config.DwsSinkConfig;

import com.huaweicloud.dws.client.DwsClient;
import com.huaweicloud.dws.client.DwsConfig;
import com.huaweicloud.dws.client.model.ConflictStrategy;
import com.huaweicloud.dws.client.model.WriteMode;
import com.huaweicloud.dws.client.op.Operate;

import java.io.IOException;
import java.util.HashSet;
import java.util.Locale;

final class HuaweiDwsClientFacade implements DwsClientFacade {
    private final String table;
    private final DwsClient client;

    HuaweiDwsClientFacade(DwsSinkConfig config, int subtaskIndex) {
        DwsConfig.Builder builder =
                DwsConfig.builder()
                        .withUrl(config.getUrl())
                        .withUsername(config.getUser())
                        .withPassword(config.getPassword())
                        .withWriteMode(parseWriteMode(config.getWriteMode()))
                        .withConflictStrategy(parseConflictStrategy(config.getConflictStrategy()))
                        .withAutoFlushBatchSize(config.getBatchSize())
                        .withAutoFlushMaxIntervalMs(config.getFlushIntervalMs())
                        .withThreadSize(config.getWriteThreadSize())
                        .withCopyWriteBatchSize(config.getCopyWriteBatchSize())
                        .withMaxFlushRetryTimes(config.getMaxRetries())
                        .withRetryBaseTime(config.getRetryBackoffMs())
                        .withCaseSensitive(config.isCaseSensitive());
        if (!config.getCompareFields().isEmpty()) {
            builder.withCompareField(new HashSet<>(config.getCompareFields()));
        }
        this.table = config.getTable();
        this.client = new DwsClient(builder.build(), "seatunnel-dws-" + subtaskIndex);
    }

    @Override
    public void write(String[] fieldNames, Object[] values) throws Exception {
        commit(client.write(table), fieldNames, values);
    }

    @Override
    public void delete(String[] fieldNames, Object[] values) throws Exception {
        commit(client.delete(table), fieldNames, values);
    }

    @Override
    public void flush() throws Exception {
        client.flush();
    }

    @Override
    public void close() throws IOException {
        client.close();
    }

    private static void commit(Operate operate, String[] fieldNames, Object[] values)
            throws Exception {
        for (int i = 0; i < fieldNames.length; i++) {
            operate.setObject(fieldNames[i], values[i]);
        }
        operate.commit();
    }

    private static WriteMode parseWriteMode(String value) {
        try {
            return WriteMode.valueOf(value.trim().toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Unsupported write_mode: " + value, e);
        }
    }

    private static ConflictStrategy parseConflictStrategy(String value) {
        try {
            return ConflictStrategy.valueOf(value.trim().toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Unsupported conflict_strategy: " + value, e);
        }
    }
}
