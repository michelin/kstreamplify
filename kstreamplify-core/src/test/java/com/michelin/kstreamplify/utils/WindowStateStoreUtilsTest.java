package com.michelin.kstreamplify.utils;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.time.Duration;
import java.time.Instant;
import java.util.Iterator;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class WindowStateStoreUtilsTest {

    private WindowStore<String, String> windowStore;

    @BeforeEach
    void setUp() {
        windowStore = mock(WindowStore.class);
    }

    @Test
    void testPut() {
        // Mock data
        String key = "testKey";
        String value = "testValue";

        // Call the put method
        WindowStateStoreUtils.put(windowStore, key, value);
        WindowStateStoreUtils.get(windowStore, key, 1);
        WindowStateStoreUtils.get(windowStore, "nothing", 1);

        // Verify that the put method of the windowStore is called with the correct arguments
        verify(windowStore, times(1)).put(eq(key), eq(value), anyLong());
    }
}
