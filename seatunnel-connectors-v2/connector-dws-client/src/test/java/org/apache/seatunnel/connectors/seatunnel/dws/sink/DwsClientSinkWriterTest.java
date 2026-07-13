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

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.dws.config.DwsSinkConfig;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DwsClientSinkWriterTest {
    private static final SeaTunnelRowType ROW_TYPE =
            new SeaTunnelRowType(
                    new String[] {"id", "name", "created_at"},
                    new SeaTunnelDataType[] {
                        BasicType.INT_TYPE, BasicType.STRING_TYPE, BasicType.STRING_TYPE
                    });

    @Test
    void routesChangelogRowsAndFlushesAtCheckpoint() throws Exception {
        FakeDwsClient client = new FakeDwsClient();
        DwsClientSinkWriter writer = new DwsClientSinkWriter(config(true), ROW_TYPE, client);

        writer.write(row(RowKind.INSERT, 1, "first", LocalDateTime.of(2026, 7, 13, 8, 0)));
        writer.write(row(RowKind.UPDATE_BEFORE, 1, "first", null));
        writer.write(row(RowKind.UPDATE_AFTER, 1, "second", null));
        writer.write(row(RowKind.DELETE, 1, "second", null));
        writer.prepareCommit();

        assertEquals(2, client.writes.size());
        assertEquals(1, client.deletes.size());
        assertEquals(1, client.flushCount);
        assertTrue(client.writes.get(0)[2] instanceof java.sql.Timestamp);
        assertEquals("second", client.writes.get(1)[1]);
    }

    @Test
    void rejectsDeleteWhenDisabled() {
        FakeDwsClient client = new FakeDwsClient();
        DwsClientSinkWriter writer = new DwsClientSinkWriter(config(false), ROW_TYPE, client);

        IOException error =
                assertThrows(
                        IOException.class,
                        () -> writer.write(row(RowKind.DELETE, 1, "first", null)));

        assertTrue(error.getMessage().contains("enable_delete=false"));
    }

    @Test
    void propagatesFlushFailure() {
        FakeDwsClient client = new FakeDwsClient();
        client.flushFailure = new IllegalStateException("flush failed");
        DwsClientSinkWriter writer = new DwsClientSinkWriter(config(true), ROW_TYPE, client);

        IOException error = assertThrows(IOException.class, writer::prepareCommit);

        assertEquals("flush failed", error.getCause().getMessage());
    }

    @Test
    void closeFlushesAndClosesClient() throws Exception {
        FakeDwsClient client = new FakeDwsClient();
        DwsClientSinkWriter writer = new DwsClientSinkWriter(config(true), ROW_TYPE, client);

        writer.close();

        assertEquals(1, client.flushCount);
        assertTrue(client.closed);
    }

    private static SeaTunnelRow row(RowKind kind, Object... fields) {
        SeaTunnelRow row = new SeaTunnelRow(fields);
        row.setRowKind(kind);
        return row;
    }

    private static DwsSinkConfig config(boolean enableDelete) {
        return new DwsSinkConfig(
                "jdbc:postgresql://localhost:8000/db",
                "user",
                "password",
                "public.target",
                "AUTO",
                "INSERT_OR_UPDATE",
                30000,
                3000,
                1,
                1000,
                3,
                1000,
                false,
                Collections.emptyList(),
                enableDelete);
    }

    private static final class FakeDwsClient implements DwsClientFacade {
        private final List<Object[]> writes = new ArrayList<>();
        private final List<Object[]> deletes = new ArrayList<>();
        private int flushCount;
        private Exception flushFailure;
        private boolean closed;

        @Override
        public void write(String[] fieldNames, Object[] values) {
            writes.add(values.clone());
        }

        @Override
        public void delete(String[] fieldNames, Object[] values) {
            deletes.add(values.clone());
        }

        @Override
        public void flush() throws Exception {
            flushCount++;
            if (flushFailure != null) {
                throw flushFailure;
            }
        }

        @Override
        public void close() {
            closed = true;
        }
    }
}
