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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.connectors.seatunnel.dws.config.DwsClientOptions;
import org.apache.seatunnel.connectors.seatunnel.dws.config.DwsSinkConfig;

import org.junit.jupiter.api.Test;

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

        DwsSinkConfig config = DwsClientOptions.from(ReadonlyConfig.fromMap(values));

        assertEquals("AUTO", config.getWriteMode());
        assertEquals("INSERT_OR_UPDATE", config.getConflictStrategy());
        assertEquals(30000, config.getBatchSize());
        assertEquals(3000, config.getFlushIntervalMs());
        assertTrue(config.isEnableDelete());
    }
}
