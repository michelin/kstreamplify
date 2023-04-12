package com.michelin.kafka.streams.init.properties;

import com.michelin.kafka.streams.starter.commons.context.KafkaStreamsExecutionContext;
import org.apache.kafka.streams.state.RocksDBConfigSetter;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.CompressionType;
import org.rocksdb.Options;

import java.util.Map;


public class RocksDBConfig implements RocksDBConfigSetter {
    public final static String ROCKSDB_CACHE_SIZE_CONFIG = "rocksdb.config.cache.size";
    public final static String ROCKSDB_WRITE_BUFFER_SIZE_CONFIG = "rocksdb.config.write.buffer.size";
    public final static String ROCKSDB_BLOCK_SIZE_CONFIG = "rocksdb.config.block.size";
    public final static String ROCKSDB_MAX_WRITE_BUFFER_CONFIG = "rocksdb.config.max.write.buffer";
    public final static String ROCKSDB_COMPRESSION_TYPE_CONFIG = "rocksdb.config.compression.type";
    public final static String ROCKSDB_CACHE_INDEX_BLOCK_ENABLED_CONFIG = "rocksdb.config.cache.index.block.enabled";
    private static final long ONE_KB = 1024L;

    public final static Long ROCKSDB_CACHE_SIZE_DEFAULT = 16 * ONE_KB * ONE_KB;
    public final static Long ROCKSDB_WRITE_BUFFER_SIZE_DEFAULT = 4 * ONE_KB * ONE_KB;
    public final static Long ROCKSDB_BLOCK_SIZE_DEFAULT = 4 * ONE_KB;
    public final static Integer ROCKSDB_MAX_WRITE_BUFFER_DEFAULT = 2;
    public final static String ROCKSDB_COMPRESSION_TYPE_DEFAULT = "";
    public final static Boolean ROCKSDB_CACHE_INDEX_BLOCK_ENABLED_DEFAULT = true;

    private org.rocksdb.Cache cache = null;

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
