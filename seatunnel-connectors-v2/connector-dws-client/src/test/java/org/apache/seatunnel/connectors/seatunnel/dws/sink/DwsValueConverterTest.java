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

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DwsValueConverterTest {
    @Test
    void convertsJavaTimeValuesForDwsJdbcConverters() {
        assertTrue(DwsValueConverter.convert(LocalDate.of(2026, 7, 13)) instanceof java.sql.Date);
        assertTrue(DwsValueConverter.convert(LocalTime.NOON) instanceof java.sql.Time);
        assertTrue(
                DwsValueConverter.convert(LocalDateTime.of(2026, 7, 13, 12, 0))
                        instanceof java.sql.Timestamp);
        assertTrue(
                DwsValueConverter.convert(
                                OffsetDateTime.of(2026, 7, 13, 12, 0, 0, 0, ZoneOffset.UTC))
                        instanceof java.sql.Timestamp);
        assertTrue(DwsValueConverter.convert(Instant.EPOCH) instanceof java.sql.Timestamp);
    }

    @Test
    void leavesOtherValuesUntouched() {
        Object value = new Object();
        assertSame(value, DwsValueConverter.convert(value));
    }
}
