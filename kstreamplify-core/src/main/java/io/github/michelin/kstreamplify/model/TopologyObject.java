package io.github.michelin.kstreamplify.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * The topology class
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class TopologyObject {
    /**
     * The topology type
     */
    private TopologyObjectType type;

    /**
     * The topology name
     */
    private String objectName;
}
