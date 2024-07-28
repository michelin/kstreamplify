package com.michelin.kstreamplify.property;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.michelin.kstreamplify.context.KafkaStreamsExecutionContext;
import com.michelin.kstreamplify.store.RocksDbConfig;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.CompressionType;
import org.rocksdb.Options;

@ExtendWith(MockitoExtension.class)
class RocksDbConfigTest {
    @Mock
    private Options options;

    @BeforeEach
    void setUp() {
        when(options.tableFormatConfig()).thenReturn(new BlockBasedTableConfig());
    }

    @Test
    void testSetConfigWithDefaultValues() {
        Map<String, Object> configs = new HashMap<>();
        RocksDbConfig rocksDbConfig = new RocksDbConfig();
        KafkaStreamsExecutionContext.registerProperties(new Properties());

        rocksDbConfig.setConfig("storeName", options, configs);

        verify(options).tableFormatConfig();
        verify(options).setTableFormatConfig(any());
        verify(options).setMaxWriteBufferNumber(RocksDbConfig.ROCKSDB_MAX_WRITE_BUFFER_DEFAULT);
        verify(options).setWriteBufferSize(RocksDbConfig.ROCKSDB_WRITE_BUFFER_SIZE_DEFAULT);
        verify(options).setCompressionType(CompressionType.NO_COMPRESSION);
    }

    @Test
    void testSetConfigWithCustomValues() {
        long cacheSize = 64 * 1024L * 1024L;
        long writeBufferSize = 8 * 1024L * 1024L;
        long blockSize = 8 * 1024L;
        int maxWriteBuffer = 4;
        boolean cacheIndexBlock = false;
        String compressionType = "lz4";

        Map<String, Object> configs = new HashMap<>();
        configs.put(RocksDbConfig.ROCKSDB_CACHE_SIZE_CONFIG, String.valueOf(cacheSize));
        configs.put(RocksDbConfig.ROCKSDB_WRITE_BUFFER_SIZE_CONFIG, String.valueOf(writeBufferSize));
        configs.put(RocksDbConfig.ROCKSDB_BLOCK_SIZE_CONFIG, String.valueOf(blockSize));
        configs.put(RocksDbConfig.ROCKSDB_MAX_WRITE_BUFFER_CONFIG, String.valueOf(maxWriteBuffer));
        configs.put(RocksDbConfig.ROCKSDB_CACHE_INDEX_BLOCK_ENABLED_CONFIG, String.valueOf(cacheIndexBlock));
        configs.put(RocksDbConfig.ROCKSDB_COMPRESSION_TYPE_CONFIG, compressionType);
        Properties properties = new Properties();
        properties.putAll(configs);
        KafkaStreamsExecutionContext.registerProperties(properties);

        RocksDbConfig rocksDbConfig = new RocksDbConfig();

        rocksDbConfig.setConfig("storeName", options, configs);

        verify(options).tableFormatConfig();
        verify(options).setTableFormatConfig(any());
        verify(options).setMaxWriteBufferNumber(maxWriteBuffer);
        verify(options).setWriteBufferSize(writeBufferSize);
        verify(options).setCompressionType(CompressionType.getCompressionType(compressionType));
    }
}
