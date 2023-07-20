package com.michelin.kstreamplify.properties;

import com.michelin.kstreamplify.context.KafkaStreamsExecutionContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.CompressionType;
import org.rocksdb.Options;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.mockito.Mockito.*;

class RocksDBConfigTest {

    @Mock
    private Options options;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        when(options.tableFormatConfig()).thenReturn(new BlockBasedTableConfig());
    }

    @Test
    void testSetConfigWithDefaultValues() {
        // Arrange
        Map<String, Object> configs = new HashMap<>();
        RocksDBConfig rocksDBConfig = new RocksDBConfig();
        KafkaStreamsExecutionContext.registerProperties(new Properties());

        // Act
        rocksDBConfig.setConfig("storeName", options, configs);

        // Assert
        verify(options, times(1)).tableFormatConfig();
        verify(options, times(1)).setTableFormatConfig(any());
        verify(options, times(1)).setMaxWriteBufferNumber(RocksDBConfig.ROCKSDB_MAX_WRITE_BUFFER_DEFAULT);
        verify(options, times(1)).setWriteBufferSize(RocksDBConfig.ROCKSDB_WRITE_BUFFER_SIZE_DEFAULT);
        verify(options, times(1)).setCompressionType(CompressionType.NO_COMPRESSION);
    }

    @Test
    void testSetConfigWithCustomValues() {
        // Arrange
        long cacheSize = 64 * 1024L * 1024L;
        long writeBufferSize = 8 * 1024L * 1024L;
        long blockSize = 8 * 1024L;
        int maxWriteBuffer = 4;
        boolean cacheIndexBlock = false;
        String compressionType = "lz4";

        Map<String, Object> configs = new HashMap<>();
        configs.put(RocksDBConfig.ROCKSDB_CACHE_SIZE_CONFIG, String.valueOf(cacheSize));
        configs.put(RocksDBConfig.ROCKSDB_WRITE_BUFFER_SIZE_CONFIG, String.valueOf(writeBufferSize));
        configs.put(RocksDBConfig.ROCKSDB_BLOCK_SIZE_CONFIG, String.valueOf(blockSize));
        configs.put(RocksDBConfig.ROCKSDB_MAX_WRITE_BUFFER_CONFIG, String.valueOf(maxWriteBuffer));
        configs.put(RocksDBConfig.ROCKSDB_CACHE_INDEX_BLOCK_ENABLED_CONFIG, String.valueOf(cacheIndexBlock));
        configs.put(RocksDBConfig.ROCKSDB_COMPRESSION_TYPE_CONFIG, compressionType);
        Properties properties = new Properties();
        properties.putAll(configs);
        KafkaStreamsExecutionContext.registerProperties(properties);

        RocksDBConfig rocksDBConfig = new RocksDBConfig();

        // Act
        rocksDBConfig.setConfig("storeName", options, configs);

        // Assert
        verify(options, times(1)).tableFormatConfig();
        verify(options, times(1)).setTableFormatConfig(any());
        verify(options, times(1)).setMaxWriteBufferNumber(maxWriteBuffer);
        verify(options, times(1)).setWriteBufferSize(writeBufferSize);
        verify(options, times(1)).setCompressionType(CompressionType.getCompressionType(compressionType));
    }
}
