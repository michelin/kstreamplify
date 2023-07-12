package com.michelin.kstreamplify.properties;

import com.michelin.kstreamplify.context.KafkaStreamsExecutionContext;
import org.apache.kafka.streams.state.RocksDBConfigSetter;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.CompressionType;
import org.rocksdb.Options;

import java.util.Map;

/**
 * The RockDB configuration class
 */
public class RocksDBConfig implements RocksDBConfigSetter {
    /**
     * The RocksDB cache size config key
     */
    public static final String ROCKSDB_CACHE_SIZE_CONFIG = "rocksdb.config.cache.size";

    /**
     * The RocksDB write buffer size config key
     */
    public static final String ROCKSDB_WRITE_BUFFER_SIZE_CONFIG = "rocksdb.config.write.buffer.size";

    /**
     * The RocksDB block size config key
     */
    public static final String ROCKSDB_BLOCK_SIZE_CONFIG = "rocksdb.config.block.size";

    /**
     * The RocksDB max write buffer config
     */
    public static final String ROCKSDB_MAX_WRITE_BUFFER_CONFIG = "rocksdb.config.max.write.buffer";

    /**
     * The RocksDB compression type config key
     */
    public static final String ROCKSDB_COMPRESSION_TYPE_CONFIG = "rocksdb.config.compression.type";

    /**
     * The RocksDB cache index block enabled config
     */
    public static final String ROCKSDB_CACHE_INDEX_BLOCK_ENABLED_CONFIG = "rocksdb.config.cache.index.block.enabled";

    /**
     * One KB in B
     */
    private static final long ONE_KB = 1024L;

    /**
     * The RocksDB default cache size
     */
    public static final Long ROCKSDB_CACHE_SIZE_DEFAULT = 16 * ONE_KB * ONE_KB;

    /**
     * The RocksDB default write buffer size
     */
    public static final Long ROCKSDB_WRITE_BUFFER_SIZE_DEFAULT = 4 * ONE_KB * ONE_KB;

    /**
     * The RocksDB default block size
     */
    public static final Long ROCKSDB_BLOCK_SIZE_DEFAULT = 4 * ONE_KB;

    /**
     * The RocksDB default max write buffer
     */
    public static final Integer ROCKSDB_MAX_WRITE_BUFFER_DEFAULT = 2;

    /**
     * The RocksDB default compression type
     */
    public static final String ROCKSDB_COMPRESSION_TYPE_DEFAULT = "";

    /**
     * The RocksDB default cache index block enabled
     */
    public static final Boolean ROCKSDB_CACHE_INDEX_BLOCK_ENABLED_DEFAULT = true;

    /**
     * The RocksDB cache
     */
    private org.rocksdb.Cache cache = null;

    /**
     * Set the RocksDB configuration
     * @param storeName The store name
     * @param options The options
     * @param configs The configs
     */
    @Override
    public void setConfig(final String storeName, final Options options, final Map<String, Object> configs) {
        long blockCacheSize = KafkaStreamsExecutionContext.getProperties().containsKey(ROCKSDB_CACHE_SIZE_CONFIG) ?
                Long.parseLong(KafkaStreamsExecutionContext.getProperties().getProperty(ROCKSDB_CACHE_SIZE_CONFIG)) : ROCKSDB_CACHE_SIZE_DEFAULT;

        long writeBufferSize = KafkaStreamsExecutionContext.getProperties().containsKey(ROCKSDB_WRITE_BUFFER_SIZE_CONFIG) ?
                Long.parseLong(KafkaStreamsExecutionContext.getProperties().getProperty(ROCKSDB_WRITE_BUFFER_SIZE_CONFIG)) : ROCKSDB_WRITE_BUFFER_SIZE_DEFAULT;

        long blockSize = KafkaStreamsExecutionContext.getProperties().containsKey(ROCKSDB_BLOCK_SIZE_CONFIG) ?
                Long.parseLong(KafkaStreamsExecutionContext.getProperties().getProperty(ROCKSDB_BLOCK_SIZE_CONFIG)) : ROCKSDB_BLOCK_SIZE_DEFAULT;

        int maxWriteBuffer = KafkaStreamsExecutionContext.getProperties().containsKey(ROCKSDB_MAX_WRITE_BUFFER_CONFIG) ?
                Integer.parseInt(KafkaStreamsExecutionContext.getProperties().getProperty(ROCKSDB_MAX_WRITE_BUFFER_CONFIG)) : ROCKSDB_MAX_WRITE_BUFFER_DEFAULT;

        boolean cacheIndexBlock = KafkaStreamsExecutionContext.getProperties().containsKey(ROCKSDB_CACHE_INDEX_BLOCK_ENABLED_CONFIG) ?
                Boolean.parseBoolean(KafkaStreamsExecutionContext.getProperties().getProperty(ROCKSDB_CACHE_INDEX_BLOCK_ENABLED_CONFIG)) : ROCKSDB_CACHE_INDEX_BLOCK_ENABLED_DEFAULT;

        String compressionType = KafkaStreamsExecutionContext.getProperties().getProperty(ROCKSDB_COMPRESSION_TYPE_CONFIG, ROCKSDB_COMPRESSION_TYPE_DEFAULT);

        if (cache == null) {
            cache = new org.rocksdb.LRUCache(blockCacheSize);
        }

        BlockBasedTableConfig tableConfig = (BlockBasedTableConfig) options.tableFormatConfig();
        tableConfig.setBlockCache(cache);
        tableConfig.setBlockSize(blockSize);
        tableConfig.setCacheIndexAndFilterBlocks(cacheIndexBlock);
        options.setTableFormatConfig(tableConfig);
        options.setMaxWriteBufferNumber(maxWriteBuffer);
        options.setWriteBufferSize(writeBufferSize);
        options.setCompressionType(CompressionType.getCompressionType(compressionType));
    }

    @Override
    public void close(String storeName, Options options) {
        cache.close();
    }
}
