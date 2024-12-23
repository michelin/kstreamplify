package com.michelin.kstreamplify.deduplication;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.WindowStore;

/**
 * Deduplication Abstract class.
 */
public abstract class AbstractDedup<V> {

    /**
     *  Migrate data from timestampKeyValueStore to dedupWindowStore.
     *
     * @param timestampKeyValueStore timestamp key value store for input data
     * @param dedupWindowStore window store for output data
     *
     */
    public void migrateDataToWindowStore(
            TimestampedKeyValueStore<String, V> timestampKeyValueStore,
            WindowStore<String, V> dedupWindowStore) {
        KeyValueIterator<String, ValueAndTimestamp<V>> iterator = timestampKeyValueStore.all();

        while (iterator.hasNext()) {
            KeyValue<String, ValueAndTimestamp<V>> entry = iterator.next();
            String key = entry.key;
            V value = entry.value.value();

            dedupWindowStore.put(key, value, entry.value.timestamp());
            timestampKeyValueStore.delete(key);
        }

        iterator.close();
    }
}
