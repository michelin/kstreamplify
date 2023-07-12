package com.michelin.kstreamplify.model;

import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * The topology expose class
 */
@Getter
@Setter
public class TopologyExposeJsonModel {
    /**
     * The input topics
     */
    private Set<String> inTopicNameList = new HashSet<>();

    /**
     * The output topics
     */
    private Set<String> outTopicNameList = new HashSet<>();

    /**
     * The state stores
     */
    private Set<String> streamStateStore = new HashSet<>();

    /**
     * The internal stream content
     */
    private List<TopologyPart> internalStreamContent = new ArrayList<>();

    /**
     * The stream name
     */
    private String streamName;
}
