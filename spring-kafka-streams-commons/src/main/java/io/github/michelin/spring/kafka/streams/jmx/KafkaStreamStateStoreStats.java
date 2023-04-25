package io.github.michelin.spring.kafka.streams.jmx;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Stream;

public class KafkaStreamStateStoreStats implements KafkaStreamStateStoreStatsMBean {


    final static Logger logger = LoggerFactory.getLogger(KafkaStreamStateStoreStats.class.getName());

    final static String METRIC_NAME = "StateStoreStat";
    private final String stateStoreName;
    private final File rootDir;
    private final Set<File> stateStoreDirectory = new TreeSet<>();


    public KafkaStreamStateStoreStats(String ssName, String ssDir, String groupId) {
        this.stateStoreName = ssName;
        var stateStoreDir = ssDir;
        if (System.getProperty("os.name").startsWith("Windows") && ssDir.equals(KafkaStreamJmxBeanRegister.DEFAULT_STATE_DIR)){
            stateStoreDir = File.listRoots()[0].getName().concat(ssDir);
        }
        rootDir = new File(stateStoreDir + File.separator + groupId);
    }

    @Override
    public long getStateStoreSize() {
        initStateSorePath();
        long size = 0L;
        for (File dir : stateStoreDirectory) {
            size += FileUtils.sizeOfDirectory(dir);
        }
        return size;
    }

    private void initStateSorePath() {
        if (stateStoreDirectory.size() == 0) {
            if (stateStoreName.isBlank()) {
                stateStoreDirectory.add(rootDir);
                logger.info("Found Global state store directory : {}", rootDir.toString());
            } else {
                try (Stream<Path> walkStream = Files.walk(rootDir.toPath())) {
                    walkStream.filter(p -> p.toFile().isDirectory()).forEach(f -> {
                        if (f.toString().endsWith(stateStoreName)) {
                            stateStoreDirectory.add(f.toFile());
                            logger.info("Found state store directory : {} for {}", f, stateStoreName);
                        }
                    });
                } catch (IOException e) {
                    logger.error("Error while searching state store local directory", e);
                } catch (InvalidPathException ipe){
                    logger.error("Invalid abstract path {}",rootDir, ipe);
                } catch (SecurityException se){
                    logger.error("Can not access directory in {} du to security policy, no read access",rootDir, se);
                }
            }
        }
    }

    @Override
    public ObjectName getName() {
        ObjectName on = null;
        var name = KafkaStreamStateStoreStats.class.getPackageName()+":type="+ METRIC_NAME +",name="+ StringUtils.defaultString(stateStoreName.isEmpty()?"Global":stateStoreName);
        try {
            on = new ObjectName(name);
        } catch (MalformedObjectNameException e) {
            logger.error("Can't initialize JMX with name : {}", name, e);
        }
        return on;
    }
}

