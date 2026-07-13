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

import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.dws.config.DwsClientOptions;
import org.apache.seatunnel.connectors.seatunnel.dws.config.DwsSinkConfig;

import java.io.IOException;

final class DwsClientSink implements SeaTunnelSink<SeaTunnelRow, Void, Void, Void> {
    private static final long serialVersionUID = 1L;

    private final DwsSinkConfig config;
    private final SeaTunnelRowType rowType;

    DwsClientSink(DwsSinkConfig config, SeaTunnelRowType rowType) {
        this.config = config;
        this.rowType = rowType;
    }

    @Override
    public String getPluginName() {
        return DwsClientOptions.CONNECTOR_IDENTITY;
    }

    @Override
    public SinkWriter<SeaTunnelRow, Void, Void> createWriter(SinkWriter.Context context)
            throws IOException {
        try {
            return new DwsClientSinkWriter(
                    config,
                    rowType,
                    new HuaweiDwsClientFacade(config, context.getIndexOfSubtask()));
        } catch (Exception e) {
            throw new IOException("Unable to create Huawei dws-client", e);
        }
    }
}
