package com.michelin.kstreamplify.model;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;

/**
 * The topology part class
 */
@Getter
@Setter
@NoArgsConstructor
public class TopologyPart {
    /**
     * The input element
     */
    TopologyObject inputElement = new TopologyObject();

    /**
     * The sub element name
     */
    TopologyObject subElementName = new TopologyObject();

    /**
     * The output element
     */
    TopologyObject outputElement = new TopologyObject();

    /**
     * The transformation
     */
    List<String> detailedTransformation = new ArrayList<>();
}
