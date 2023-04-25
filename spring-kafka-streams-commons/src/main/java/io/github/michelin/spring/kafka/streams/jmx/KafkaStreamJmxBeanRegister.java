package io.github.michelin.spring.kafka.streams.jmx;

import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.NotCompliantMBeanException;
import java.lang.management.ManagementFactory;
import java.util.Properties;
import java.util.TreeSet;

public class KafkaStreamJmxBeanRegister {

    public static final String DEFAULT_STATE_DIR = "/tmp/kafka-streams";
    MBeanServer mbs;
    TreeSet<String> stateStore;
    Properties prt;

    final static Logger logger = LoggerFactory.getLogger(KafkaStreamJmxBeanRegister.class.getName());

    public KafkaStreamJmxBeanRegister(Topology topology, Properties properties) {
        stateStore = new TreeSet<>();
        prt = properties;
        topology.describe().subtopologies().forEach(this::handleSubTopology);
        stateStore.add("");
        mbs = ManagementFactory.getPlatformMBeanServer();
    }

    private void handleSubTopology(TopologyDescription.Subtopology subtopology) {
        subtopology.nodes().forEach(this::handleNode);
    }

    private void handleNode(TopologyDescription.Node node) {
        if(node instanceof TopologyDescription.Processor processor) {
            stateStore.addAll(processor.stores());
        }
    }

    public void register() {
        try {
            for(var ss : stateStore) {
                KafkaStreamStateStoreStats mBean = new KafkaStreamStateStoreStats(ss,prt.getProperty(StreamsConfig.STATE_DIR_CONFIG, DEFAULT_STATE_DIR), prt.getProperty(StreamsConfig.APPLICATION_ID_CONFIG));
                mbs.registerMBean(mBean, mBean.getName());
            }
        } catch (NotCompliantMBeanException | InstanceAlreadyExistsException | MBeanRegistrationException e) {
            logger.error("Error while registering mBean", e);
        }
    }



}
