/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.michelin.kstreamplify.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class WindowStateStoreUtilsTest {
    @Mock
    private WindowStore<String, String> windowStore;

    @Mock
    private WindowStoreIterator<String> iterator;

    @Test
    void shouldReturnNull() {
        when(windowStore.backwardFetch(anyString(), any(), any())).thenReturn(null);

        String result = WindowStateStoreUtils.get(windowStore, "testKey", 1);
        assertNull(result);
    }

    @Test
    void shouldPutAndGetFromWindowStore() {
        String value = "testValue";

        when(iterator.hasNext()).thenReturn(true).thenReturn(false);

        when(iterator.next()).thenReturn(KeyValue.pair(1L, value));

        when(windowStore.backwardFetch(anyString(), any(), any())).thenReturn(iterator);

        String key = "testKey";
        WindowStateStoreUtils.put(windowStore, key, value);
        String result = WindowStateStoreUtils.get(windowStore, key, 1);
        String nullResult = WindowStateStoreUtils.get(windowStore, "nothing", 1);

        assertEquals("testValue", result);
        assertNull(nullResult);
        verify(windowStore).put(eq(key), eq(value), anyLong());
    }
}
