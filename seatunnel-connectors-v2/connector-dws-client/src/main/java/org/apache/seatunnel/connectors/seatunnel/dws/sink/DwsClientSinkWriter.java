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

import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.dws.config.DwsSinkConfig;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

final class DwsClientSinkWriter implements SinkWriter<SeaTunnelRow, Void, Void> {
    private final DwsSinkConfig config;
    private final String[] fieldNames;
    private final DwsClientFacade client;

    DwsClientSinkWriter(DwsSinkConfig config, SeaTunnelRowType rowType, DwsClientFacade client) {
        this.config = config;
        this.fieldNames = rowType.getFieldNames();
        this.client = client;
    }

    @Override
    public void write(SeaTunnelRow row) throws IOException {
        Object[] values = new Object[fieldNames.length];
        for (int i = 0; i < fieldNames.length; i++) {
            values[i] = DwsValueConverter.convert(row.getField(i));
        }
        try {
            RowKind kind = row.getRowKind();
            if (kind == RowKind.UPDATE_BEFORE) {
                return;
            }
            if (kind == RowKind.DELETE) {
                if (!config.isEnableDelete()) {
                    throw new IOException("Received DELETE row while enable_delete=false");
                }
                client.delete(fieldNames, values);
                return;
            }
            client.write(fieldNames, values);
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException("Failed to submit row to Huawei dws-client", e);
        }
    }

    @Override
    public Optional<Void> prepareCommit() throws IOException {
        flushClient();
        return Optional.empty();
    }

    @Override
    public List<Void> snapshotState(long checkpointId) throws IOException {
        flushClient();
        return Collections.emptyList();
    }

    @Override
    public void abortPrepare() {
        // dws-client manages its own statement/import lifecycle.
    }

    @Override
    public void close() throws IOException {
        IOException failure = null;
        try {
            flushClient();
        } catch (IOException e) {
            failure = e;
        }
        try {
            client.close();
        } catch (IOException e) {
            if (failure == null) {
                failure = e;
            } else {
                failure.addSuppressed(e);
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    private void flushClient() throws IOException {
        try {
            client.flush();
        } catch (Exception e) {
            throw new IOException("Failed to flush Huawei dws-client", e);
        }
    }
}
