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
    void shouldPutAndGetFromWindowStore() {
        String value = "testValue";

        when(iterator.hasNext())
            .thenReturn(true)
            .thenReturn(false);

        when(iterator.next())
            .thenReturn(KeyValue.pair(1L, value));

        when(windowStore.backwardFetch(anyString(), any(), any()))
            .thenReturn(iterator);

        // Call the put method
        String key = "testKey";
        WindowStateStoreUtils.put(windowStore, key, value);
        String result = WindowStateStoreUtils.get(windowStore, key, 1);
        String nullResult = WindowStateStoreUtils.get(windowStore, "nothing", 1);

        // Verify that the put method of the windowStore is called with the correct arguments
        assertEquals("testValue", result);
        assertNull(nullResult);
        verify(windowStore).put(eq(key), eq(value), anyLong());
    }
}
