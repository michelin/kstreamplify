package io.github.michelin.spring.kafka.streams.model;

import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Getter
@Setter
public class TopologyExposeJsonModel {
    private Set<String> inTopicNameList = new HashSet<>();
    private Set<String> outTopicNameList = new HashSet<>();
    private Set<String> streamStateStore = new HashSet<>();
    private List<TopologyPart> internalStreamContent = new ArrayList<>();
    private String streamName;
}
