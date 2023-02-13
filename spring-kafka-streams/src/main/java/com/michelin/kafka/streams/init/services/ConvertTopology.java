package com.michelin.kafka.streams.init.services;

import com.michelin.kafka.streams.init.model.TopologyPart;
import com.michelin.kafka.streams.init.model.TopologyExposeJsonModel;
import com.michelin.kafka.streams.init.model.TopologyObject;
import com.michelin.kafka.streams.init.model.TopologyObjectType;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyDescription;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ConvertTopology {
    private static final String SINK = "-SINK-";
    private static final String SOURCE = "-SOURCE-";

    private ConvertTopology() { }

    public static TopologyExposeJsonModel convertTopologyForRest(String streamName, Topology topology) {
        var result = new TopologyExposeJsonModel();
        result.setStreamName(streamName);
        for (TopologyDescription.Subtopology subTopology : topology.describe().subtopologies()) {
            handleSubTopology(subTopology, result);
        }
        return result;
    }

    private static void handleSubTopology(TopologyDescription.Subtopology subTopology, TopologyExposeJsonModel obj){
        Set<String> nodeProcessed = new HashSet<>();
        for (TopologyDescription.Node node : subTopology.nodes()) {
            if (!nodeProcessed.contains(node.name())){
                handleNode(nodeProcessed, obj, node, new TopologyPart());
            }
        }
    }

    private static void handleNode(Set<String> nodeProcessed, TopologyExposeJsonModel obj, TopologyDescription.Node node, TopologyPart currentNodeAncestorsPath) {
        nodeProcessed.add(node.name());
        List<String> currentElements = currentNodeAncestorsPath.getDetailedTransformation();
        currentElements.add(node.name());
        currentNodeAncestorsPath.setDetailedTransformation(currentElements);

        TopologyObject elementName = currentNodeAncestorsPath.getSubElementName();

        if (node.successors().size() > 1) {
            var t = obj.getInternalStreamContent();

            elementName.setObjectName(obj.getStreamName()+"\\r\\n"+"Element "+(obj.getInternalStreamContent().size()+1));
            elementName.setType(TopologyObjectType.STREAM);
            currentNodeAncestorsPath.setSubElementName(elementName);
            t.add(currentNodeAncestorsPath);

            obj.setInternalStreamContent(t);
            currentNodeAncestorsPath = new TopologyPart();
            currentNodeAncestorsPath.setInputElement(elementName);
        }

        if (node.successors().isEmpty()) {
            var t = obj.getInternalStreamContent();

            elementName.setObjectName(obj.getStreamName()+"\\r\\n"+"Element "+ (obj.getInternalStreamContent().size() + 1));
            elementName.setType(TopologyObjectType.STREAM);
            currentNodeAncestorsPath.setSubElementName(elementName);

            TopologyObject elementNameSink = new TopologyObject();
            elementNameSink.setType(TopologyObjectType.TOPIC_OUT);
            currentNodeAncestorsPath.setOutputElement(elementNameSink);

            if (node.name().contains(SINK)) {
                TopologyDescription.Sink sink = (TopologyDescription.Sink)node;
                elementNameSink.setObjectName(sink.topic());
            }

            t.add(currentNodeAncestorsPath);

            obj.setInternalStreamContent(t);
            currentNodeAncestorsPath = new TopologyPart();
            currentNodeAncestorsPath.setInputElement(elementName);
        }

        if (node.name().contains(SOURCE)) {
            TopologyDescription.Source source = (TopologyDescription.Source)node;
            var t = obj.getInTopicNameList();
            TopologyObject elementNameSource = new TopologyObject();
            elementNameSource.setType(TopologyObjectType.TOPIC_IN);
            for (String topic : source.topicSet()){
                elementNameSource.setObjectName(topic);
                t.add(topic);
            }
            currentNodeAncestorsPath.setInputElement(elementNameSource);

            obj.setInTopicNameList(t);
        } else {
            if (node.name().contains(SINK)) {
                TopologyObject elementNameSink = new TopologyObject();
                elementNameSink.setType(TopologyObjectType.TOPIC_OUT);

                TopologyDescription.Sink sink = (TopologyDescription.Sink)node;
                elementNameSink.setObjectName(sink.topic());
                var t = obj.getOutTopicNameList();
                t.add(sink.topic());
                obj.setOutTopicNameList(t);
                currentNodeAncestorsPath.setOutputElement(elementNameSink);
            }
        }

        for (TopologyDescription.Node nodeBelow : node.successors()) {
            handleNode(nodeProcessed, obj, nodeBelow, currentNodeAncestorsPath);
            currentNodeAncestorsPath = new TopologyPart();
            currentNodeAncestorsPath.setInputElement(elementName);
        }
    }
}
