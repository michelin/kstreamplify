<div align="center">

# Kstreamplify

[![GitHub Build](https://img.shields.io/github/actions/workflow/status/michelin/kstreamplify/push_main.yml?branch=main&logo=github&style=for-the-badge)](https://img.shields.io/github/actions/workflow/status/michelin/kstreamplify/push_main.yml)
[![Sonatype Nexus (Releases)](https://img.shields.io/nexus/r/com.michelin/kstreamplify?server=https%3A%2F%2Fs01.oss.sonatype.org%2F&style=for-the-badge&logo=sonatype)](https://central.sonatype.com/search?q=com.michelin.kstreamplify&sort=name)
[![GitHub Release](https://img.shields.io/github/v/release/michelin/kstreamplify?logo=github&style=for-the-badge)](https://github.com/michelin/kstreamplify/releases)
[![GitHub Stars](https://img.shields.io/github/stars/michelin/kstreamplify?logo=github&style=for-the-badge)](https://github.com/michelin/kstreamplify)
[![SonarCloud Coverage](https://img.shields.io/sonar/coverage/michelin_kstreamplify?logo=sonarcloud&server=https%3A%2F%2Fsonarcloud.io&style=for-the-badge)](https://sonarcloud.io/component_measures?id=michelin_kstreamplify&metric=coverage&view=list)
[![SonarCloud Tests](https://img.shields.io/sonar/tests/michelin_kstreamplify/main?server=https%3A%2F%2Fsonarcloud.io&style=for-the-badge&logo=sonarcloud)](https://sonarcloud.io/component_measures?metric=tests&view=list&id=michelin_kstreamplify)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg?logo=apache&style=for-the-badge)](https://opensource.org/licenses/Apache-2.0)

[Overview](#overview) • [Getting Started](#bootstrapping)

Swiftly build and enhance your Kafka Streams applications.

Kstreamplify adds extra features to Kafka Streams, simplifying development so you can write applications with minimal effort and stay focused on business implementation.

<img src=".readme/topology.gif" alt="KStreamplify application" />

</div>

## Table of Contents

* [Overview](#overview)
* [Dependencies](#dependencies)
  * [Java](#java)
  * [Spring Boot](#spring-boot)
  * [Unit Test](#unit-test)
* [Bootstrapping](#bootstrapping)
  * [Create your first Kstreamplify application](#create-your-first-kstreamplify-application) 
  * [Properties Injection](#properties-injection)
* [Avro Serializer and Deserializer](#avro-serializer-and-deserializer)
* [Error Handling](#error-handling)
  * [Set up DLQ Topic](#set-up-dlq-topic)
  * [Processing Errors](#processing-errors)
  * [Production and Deserialization Errors](#production-and-deserialization-errors)
  * [Avro Schema](#avro-schema)
  * [Uncaught Exception Handler](#uncaught-exception-handler)
* [Web Services](#web-services)
  * [Topology](#topology)
  * [State Stores](#state-stores)
  * [Kubernetes](#kubernetes)
* [TopicWithSerde API](#topicwithserde-api)
  * [Declaration](#declaration)
  * [Prefix](#prefix)
* [Interactive Queries](#interactive-queries)
  * [Configuration](#configuration)
  * [Services](#services)
* [Hooks](#hooks)
  * [On Start](#on-start)
* [Deduplication](#deduplication)
  * [By Key](#by-key)
  * [By Key and Value](#by-key-and-value)
  * [By Predicate](#by-predicate)
* [Open Telemetry](#open-telemetry)
* [Swagger](#swagger)
* [Testing](#testing)
  * [Create your first test](#create-your-first-test)
  * [Override Properties](#override-properties)
* [Motivation](#motivation)
* [Contribution](#contribution)

## Overview

Wondering what makes Kstreamplify stand out? Here are some of the key features that make it a must-have for Kafka Streams:

- **🚀 Bootstrapping**: Automatic startup, configuration, and initialization of Kafka Streams is handled for you. Focus on
business implementation rather than the setup.

- **📝 Avro Serializer and Deserializer**: Common serializers and deserializers for Avro.

- **⛑️ Error Handling**: Catch and route errors to a dead-letter queue (DLQ) topic.

- **☸️ Kubernetes**: Accurate readiness and liveness probes for Kubernetes deployment.

- **🤿 Interactive Queries**: Dive into Kafka Streams state stores.

- **🫧 Deduplication**: Remove duplicate events from a stream.

- **🧪 Testing**: Automatic Topology Test Driver setup. Start writing your tests with minimal effort.

## Dependencies

Kstreamplify offers three dependencies, all compatible with Java 17 and 21.

### Java

[![javadoc](https://javadoc.io/badge2/com.michelin/kstreamplify-core/javadoc.svg?style=for-the-badge)](https://javadoc.io/doc/com.michelin/kstreamplify-core)

To include the core Kstreamplify library in your project, add the following dependency:

```xml
<dependency>
    <groupId>com.michelin</groupId>
    <artifactId>kstreamplify-core</artifactId>
    <version>${kstreamplify.version}</version>
</dependency>
```

### Spring Boot

[![javadoc](https://javadoc.io/badge2/com.michelin/kstreamplify-spring-boot/javadoc.svg?style=for-the-badge&)](https://javadoc.io/doc/com.michelin/kstreamplify-spring-boot)

For Spring Boot applications, use the following dependency:

```xml
<dependency>
    <groupId>com.michelin</groupId>
    <artifactId>kstreamplify-spring-boot</artifactId>
    <version>${kstreamplify.version}</version>
</dependency>
```

The dependency is compatible with Spring Boot 3.

### Unit Test

[![javadoc](https://javadoc.io/badge2/com.michelin/kstreamplify-core-test/javadoc.svg?style=for-the-badge&)](https://javadoc.io/doc/com.michelin/kstreamplify-core-test)

For both Java and Spring Boot dependencies, a testing dependency is available to facilitate testing:

```xml
<dependency>
    <groupId>com.michelin</groupId>
    <artifactId>kstreamplify-core-test</artifactId>
    <version>${kstreamplify.version}</version>
    <scope>test</scope>
</dependency>
```

## Bootstrapping

Kstreamplify simplifies the bootstrapping of Kafka Streams applications by handling the startup, configuration, and
initialization of Kafka Streams for you.

### Create your first Kstreamplify application

Define a `KafkaStreamsStarter` bean within your Spring Boot context and override the `KafkaStreamsStarter#topology()` method:

```java
@Component
public class MyKafkaStreams extends KafkaStreamsStarter {
    @Override
    public void topology(StreamsBuilder streamsBuilder) {
        // Your topology
    }

    @Override
    public String dlqTopic() {
        return "DLQ_TOPIC";
    }
}
```

### Properties Injection

You can define all your Kafka Streams properties directly from the `application.yml` file as follows:

```yml
kafka:
  properties:
    bootstrap.servers: localhost:9092
    schema.registry.url: http://localhost:8081
    application.id: myKafkaStreams
    client.id: myKafkaStreams
    state.dir: /tmp/my-kafka-streams
    acks: all
    auto.offset.reset: earliest
    avro.remove.java.properties: true
```

Note that all the Kafka Streams properties are prefixed with `kafka.properties`.

## Avro Serializer and Deserializer

Whenever you need to serialize or deserialize records with Avro schemas, you can use the `SerdesUtils` class as follows:

```java
SerdesUtils.<MyAvroValue>getValueSerde()
```

or

```java
SerdesUtils.<MyAvroValue>getKeySerde()
```

Here is an example of using these methods in your topology:

```java
@Component
public class MyKafkaStreams extends KafkaStreamsStarter {
    @Override
    public void topology(StreamsBuilder streamsBuilder) {
        streamsBuilder
            .stream("INPUT_TOPIC", Consumed.with(Serdes.String(), SerdesUtils.<KafkaPerson>getValueSerde()))
            .to("OUTPUT_TOPIC", Produced.with(Serdes.String(), SerdesUtils.<KafkaPerson>getValueSerde()));
    }
}
```

## Error Handling

Kstreamplify provides the ability to handle errors and route them to a dead-letter queue (DLQ) topic.

### Set up DLQ Topic

Override the `dlqTopic` method and return the name of your DLQ topic:

```java
@Component
public class MyKafkaStreams extends KafkaStreamsStarter {
    @Override
    public void topology(StreamsBuilder streamsBuilder) {
    }

    @Override
    public String dlqTopic() {
        return "DLQ_TOPIC";
    }
}
```

### Processing Errors

To handle processing errors and route them to the DLQ topic, you can use the `ProcessingResult` class.

```java
@Component
public class MyKafkaStreams extends KafkaStreamsStarter {
    @Override
    public void topology(StreamsBuilder streamsBuilder) {
        KStream<String, KafkaPerson> stream = streamsBuilder
            .stream("INPUT_TOPIC", Consumed.with(Serdes.String(), SerdesUtils.getValueSerde()));

        TopologyErrorHandler
            .catchErrors(stream.mapValues(MyKafkaStreams::toUpperCase))
            .to("OUTPUT_TOPIC", Produced.with(Serdes.String(), SerdesUtils.getValueSerde()));
    }

    @Override
    public String dlqTopic() {
        return "DLQ_TOPIC";
    }

    private static ProcessingResult<KafkaPerson, KafkaPerson> toUpperCase(KafkaPerson value) {
        try {
            value.setLastName(value.getLastName().toUpperCase());
            return ProcessingResult.success(value);
        } catch (Exception e) {
            return ProcessingResult.fail(e, value, "Something bad happened...");
        }
    }
}
```

The map values processing returns a `ProcessingResult<V, V2>`, where:

- The first parameter is the type of the new value after a successful transformation.
- The second parameter is the type of the current value for which the transformation failed.

Use the following to mark the result as successful:

```java
ProcessingResult.success(value);
```

Or the following in a catch clause to mark the result as failed:

```java
ProcessingResult.fail(e, value, "Something bad happened...");
```

Invoke the `TopologyErrorHandler#catchErrors()` by passing the stream to route the failed records to the DLQ topic.
A healthy stream is then returned and can be further processed.

### Production and Deserialization Errors

Production and deserialization handlers are provided to send errors to the DLQ topic.
Add the following properties to your `application.yml` file:

```yml
kafka:
  properties:
    default.production.exception.handler: com.michelin.kstreamplify.error.DlqProductionExceptionHandler
    default.deserialization.exception.handler: com.michelin.kstreamplify.error.DlqDeserializationExceptionHandler
```

### Avro Schema

An Avro schema needs to be deployed in a Schema Registry on top of the DLQ topic. It is
available [here](https://github.com/michelin/kstreamplify/blob/main/kstreamplify-core/src/main/avro/kafka-error.avsc).

### Uncaught Exception Handler

The default uncaught exception handler catches all uncaught exceptions and shuts down the client.

To change this behaviour, override the `KafkaStreamsStarter#uncaughtExceptionHandler()` method and return your own
uncaught exception handler.

```java
@Override
public StreamsUncaughtExceptionHandler uncaughtExceptionHandler() {
    return throwable -> {
        return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_APPLICATION;
    };
}
```

## Web Services

Kstreamplify provides web services on top of your Kafka Streams application.
They are available through the [Swagger UI](#swagger).

### Topology

The topology endpoint return the Kafka Streams topology description. It is available at `/topology` by default.

The path can be customized by setting the following property:

```yml
topology:
  path: custom-topology
```

### State Stores

A list of endpoints to query the state stores of your Kafka Streams application is available.
It uses [interactive queries](https://docs.confluent.io/platform/current/streams/developer-guide/interactive-queries.html) and 
handle state stores being on different Kafka Streams instances by providing an [RPC layer](https://docs.confluent.io/platform/current/streams/developer-guide/interactive-queries.html#adding-an-rpc-layer-to-your-application).

Here is the list of supported state store types:
- Key-Value store
- Window store

Only state stores with String keys are supported.

### Kubernetes

Readiness and liveness endpoints provide probes for Kubernetes deployment based on the Kafka Streams state.
They are available at `/ready` and `/liveness` by default.

The path can be customized by setting the following properties:

```yml
kubernetes:
  readiness:
    path: custom-readiness
  liveness:
    path: custom-liveness
```

## TopicWithSerde API

Kstreamplify provides an API called `TopicWithSerde` that unifies all the consumption and production points and
deals with topics being owned by different teams across different environments.

### Declaration

Declare your consumption and production points in a separate class.
It requires a topic name, a key SerDe, and a value SerDe.

```java
public static TopicWithSerde<String, KafkaPerson> inputTopic() {
    return new TopicWithSerde<>(
        "INPUT_TOPIC",
        Serdes.String(),
        SerdesUtils.getValueSerdes()
    );
}

public static TopicWithSerde<String, KafkaPerson> outputTopic() {
    return new TopicWithSerde<>(
        "OUTPUT_TOPIC",
        Serdes.String(),
        SerdesUtils.getValueSerdes()
    );
}
```

Use it in your topology:

```java
@Slf4j
@Component
public class MyKafkaStreams extends KafkaStreamsStarter {
    @Override
    public void topology(StreamsBuilder streamsBuilder) {
        KStream<String, KafkaPerson> stream = inputTopic().stream(streamsBuilder);
        outputTopic().produce(stream);
    }
}
```

### Prefix

The `TopicWithSerde` API power is to handle topics owned by different teams across different environments without altering the topology.
It uses prefixes to differentiate teams and topic ownership.

In the `application.yml` file, declare the prefixes in a `key: value` format:

```yml
kafka:
  properties:
    prefix:
      self: staging.team1.
      team2: staging.team2.
      team3: staging.team3.
```

Include the prefix `TopicWithSerde` declaration:

```java
public static TopicWithSerde<String, KafkaPerson> inputTopic() {
    return new TopicWithSerde<>(
        "INPUT_TOPIC",
        "team1",
        Serdes.String(),
        SerdesUtils.getValueSerdes()
    );
}
```

> The topic `staging.team1.INPUT_TOPIC` will be consumed when running the application with the staging `application.yml` file.

When not specifying a prefix, `self` is used by default.

## Interactive Queries

Kstreamplify wants to ease the use of [interactive queries](https://docs.confluent.io/platform/current/streams/developer-guide/interactive-queries.html) in Kafka Streams application.

### Configuration

The "[application.server](https://docs.confluent.io/platform/current/streams/developer-guide/config-streams.html#application-server)" property value is determined from different sources by the following order of priority:

1. The value of an environment variable whose name is defined by the `application.server.var.name` property.

```yml
kafka:
  properties:
    application.server.var.name: MY_APPLICATION_SERVER
```

2. The value of a default environment variable named `APPLICATION_SERVER`.
3. `localhost`.

### Services

You can leverage the interactive queries services used by the web services layer to serve your own needs.

```java
@Component
public class MyService {
    @Autowired
    KeyValueStoreService keyValueStoreService;

    @Autowired
    WindowStoreService windowStoreService;
}
```

## Hooks

Kstreamplify offers the flexibility to execute custom code through hooks.

### On Start

The `On Start` hook allows you to execute code before starting the Kafka Streams instance.

```java
@Component
public class MyKafkaStreams extends KafkaStreamsStarter {
    @Override
    public void onStart(KafkaStreams kafkaStreams) {
        // Do something before starting the Kafka Streams instance
    }
}
```

## Deduplication

Kstreamplify facilitates deduplication of a stream through the `DeduplicationUtils` class, based on various criteria
and within a specified time frame.

All deduplication methods return a `KStream<String, ProcessingResult<V,V2>` so you can redirect the result to the
`TopologyErrorHandler#catchErrors()`.

**Note**: Only streams with String keys and Avro values are supported.

### By Key

```java
@Component
public class MyKafkaStreams extends KafkaStreamsStarter {
    @Override
    public void topology(StreamsBuilder streamsBuilder) {
        KStream<String, KafkaPerson> myStream = streamsBuilder
            .stream("INPUT_TOPIC");

        DeduplicationUtils
            .deduplicateKeys(streamsBuilder, myStream, Duration.ofDays(60))
            .to("OUTPUT_TOPIC");
    }
}
```

### By Key and Value

```java
@Component
public class MyKafkaStreams extends KafkaStreamsStarter {
    @Override
    public void topology(StreamsBuilder streamsBuilder) {
        KStream<String, KafkaPerson> myStream = streamsBuilder
            .stream("INPUT_TOPIC");

        DeduplicationUtils
            .deduplicateKeyValues(streamsBuilder, myStream, Duration.ofDays(60))
            .to("OUTPUT_TOPIC");
    }
}
```

### By Predicate

```java
@Component
public class MyKafkaStreams extends KafkaStreamsStarter {
    @Override
    public void topology(StreamsBuilder streamsBuilder) {
        KStream<String, KafkaPerson> myStream = streamsBuilder
            .stream("INPUT_TOPIC");

        DeduplicationUtils
            .deduplicateWithPredicate(streamsBuilder, myStream, Duration.ofDays(60),
                value -> value.getFirstName() + "#" + value.getLastName())
            .to("OUTPUT_TOPIC");
    }
}
```

The given predicate will be used as a key in the window store. The stream will be deduplicated based on the predicate.

## Open Telemetry

The Kstreamplify Spring Boot module simplifies the integration of [Open Telemetry](https://opentelemetry.io/) 
and its Java agent in Kafka Streams applications by binding all Kafka Streams metrics to the Spring Boot registry.

You can run your application with the Open Telemetry Java agent by including the following JVM options:

```console
-javaagent:/opentelemetry-javaagent.jar -Dotel.traces.exporter=otlp -Dotel.logs.exporter=otlp -Dotel.metrics.exporter=otlp
```

It also facilitates the addition of custom tags to the metrics, allowing you to use them to organize your metrics in
your Grafana dashboard.

```console
-Dotel.resource.attributes=environment=production,service.namespace=myNamespace,service.name=myKafkaStreams,category=orders
```

All the tags specified in the `otel.resource.attributes` property will be included in the metrics and can be observed in
the logs during the application startup.

## Swagger

The Kstreamplify Spring Boot dependency uses [Springdoc](https://springdoc.org/) to generate an API documentation for Kafka Streams.

By default:
- The Swagger UI page is available at `http://host:port/swagger-ui/index.html`.
- The OpenAPI description is available at `http://host:port/v3/api-docs`.

Both can be customized by using the [Springdoc properties](https://springdoc.org/#properties).

## Testing

Kstreamplify eases the use of the Topology Test Driver for testing Kafka Streams application.

### Create your first test

You can create a test class that extends `KafkaStreamsStarterTest`, override
the `KafkaStreamsStarterTest#getKafkaStreamsStarter()` to provide your `KafkaStreamsStarter` implementation, 
and start writing your tests.

```java
public class MyKafkaStreamsTest extends KafkaStreamsStarterTest {
    private TestInputTopic<String, KafkaPerson> inputTopic;
    private TestOutputTopic<String, KafkaPerson> outputTopic;

    @Override
    protected KafkaStreamsStarter getKafkaStreamsStarter() {
        return new MyKafkaStreams();
    }

    @BeforeEach
    void setUp() {
        inputTopic = testDriver.createInputTopic("INPUT_TOPIC", new StringSerializer(),
            SerdesUtils.<KafkaPerson>getValueSerde().serializer());

        outputTopic = testDriver.createOutputTopic("OUTPUT_TOPIC", new StringDeserializer(),
            SerdesUtils.<KafkaPerson>getValueSerde().deserializer());
    }

    @Test
    void shouldUpperCase() {
        inputTopic.pipeInput("1", person);
        List<KeyValue<String, KafkaPerson>> results = outputTopic.readKeyValuesToList();
        assertThat(results.get(0).value.getFirstName()).isEqualTo("FIRST NAME");
        assertThat(results.get(0).value.getLastName()).isEqualTo("LAST NAME");
    }

    @Test
    void shouldFailAndRouteToDlqTopic() {
        inputTopic.pipeInput("1", person);
        List<KeyValue<String, KafkaError>> errors = dlqTopic.readKeyValuesToList();
        assertThat(errors.get(0).key).isEqualTo("1");
        assertThat(errors.get(0).value.getContextMessage()).isEqualTo("Something bad happened...");
        assertThat(errors.get(0).value.getOffset()).isZero();
    }
}
```

### Override Properties

Kstreamplify runs the tests with default properties. 
It is possible to provide additional properties or override the default ones:

```java
public class MyKafkaStreamsTest extends KafkaStreamsStarterTest {
    @Override
    protected Map<String, String> getSpecificProperties() {
        return Map.of(
            STATE_DIR_CONFIG, "/tmp/kafka-streams"
        );
    }
}
```

## Motivation

Developing applications with Kafka Streams can be challenging and often raises many questions for developers. It
involves considerations such as efficient bootstrapping of Kafka Streams applications, handling unexpected business
issues, and integrating Kubernetes probes, among others.

To assist developers in overcoming these challenges, we have built this library. Our aim is to provide a comprehensive
solution that simplifies the development process and addresses common pain points encountered while working with Kafka
Streams.

## Contribution

We welcome contributions from the community! Before you get started, please take a look at
our [contribution guide](https://github.com/michelin/kstreamplify/blob/master/CONTRIBUTING.md) to learn about our
guidelines and best practices. We appreciate your help in making Kstreamplify a better library for everyone.
