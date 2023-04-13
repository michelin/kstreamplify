package io.github.michelin.spring.kafka.streams.model;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;

@Getter
@Setter
@NoArgsConstructor
public class TopologyPart {
    TopologyObject inputElement = new TopologyObject();

    TopologyObject subElementName = new TopologyObject();

    TopologyObject outputElement = new TopologyObject();

    List<String> detailedTransformation = new ArrayList<>();
}
