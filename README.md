<div align="center">

<img src=".readme/logo.svg" alt="Kstreamplify"/>

# Kstreamplify

[![GitHub Build](https://img.shields.io/github/actions/workflow/status/michelin/kstreamplify/build.yml?branch=main&logo=github&style=for-the-badge)](https://img.shields.io/github/actions/workflow/status/michelin/kstreamplify/build.yml)
[![Maven Central](https://img.shields.io/maven-central/v/com.michelin/kstreamplify?style=for-the-badge&logo=apache-maven&label=Maven%20Central)](https://central.sonatype.com/search?q=com.michelin.kstreamplify&sort=name)
![Supported Java Versions](https://img.shields.io/badge/Java-17--21--25-blue.svg?style=for-the-badge&logo=openjdk)
[![Kafka Version](https://img.shields.io/badge/dynamic/xml?url=https%3A%2F%2Fraw.githubusercontent.com%2Fmichelin%2Fkstreamplify%2Fmain%2Fpom.xml&query=%2F*%5Blocal-name()%3D'project'%5D%2F*%5Blocal-name()%3D'properties'%5D%2F*%5Blocal-name()%3D'kafka.version'%5D%2Ftext()&style=for-the-badge&logo=apachekafka&label=version)](https://github.com/michelin/kstreamplify/blob/main/pom.xml)
[![Spring Boot Version](https://img.shields.io/badge/dynamic/xml?url=https%3A%2F%2Fraw.githubusercontent.com%2Fmichelin%2Fkstreamplify%2Fmain%2Fpom.xml&query=%2F*%5Blocal-name()%3D'project'%5D%2F*%5Blocal-name()%3D'properties'%5D%2F*%5Blocal-name()%3D'spring-boot.version'%5D%2Ftext()&style=for-the-badge&logo=spring-boot&label=version)](https://github.com/michelin/kstreamplify/blob/main/pom.xml)
[![GitHub Stars](https://img.shields.io/github/stars/michelin/kstreamplify?logo=github&style=for-the-badge)](https://github.com/michelin/kstreamplify)
[![SonarCloud Coverage](https://img.shields.io/sonar/coverage/michelin_kstreamplify?logo=sonarcloud&server=https%3A%2F%2Fsonarcloud.io&style=for-the-badge)](https://sonarcloud.io/component_measures?id=michelin_kstreamplify&metric=coverage&view=list)
[![SonarCloud Tests](https://img.shields.io/sonar/tests/michelin_kstreamplify/main?server=https%3A%2F%2Fsonarcloud.io&style=for-the-badge&logo=sonarcloud)](https://sonarcloud.io/component_measures?metric=tests&view=list&id=michelin_kstreamplify)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg?logo=apache&style=for-the-badge)](https://opensource.org/licenses/Apache-2.0)

[Overview](#overview) • [Getting Started](#getting-started)

Swiftly build and enhance your Kafka Streams applications.

Kstreamplify adds extra features to Kafka Streams, simplifying development so you can write applications with minimal effort and stay focused on business implementation.

<img src=".readme/topology.gif" alt="Kstreamplify application" />

</div>

## Table of Contents

* [Overview](#overview)
* [Getting Started](#getting-started)
  * [Spring Boot](#spring-boot)
  * [Java](#java)
  * [Unit Test](#unit-test)
    * [Override Properties](#override-properties)
* [Avro Serializer and Deserializer](#avro-serializer-and-deserializer)
* [Error Handling](#error-handling)
  * [Set up DLQ Topic](#set-up-dlq-topic)
  * [Handling Processing Errors](#handling-processing-errors)
    * [DSL](#dsl)
    * [Processor API](#processor-api)
  * [Production and Deserialization Errors](#production-and-deserialization-errors)
  * [Avro Schema](#avro-schema)
  * [Uncaught Exception Handler](#uncaught-exception-handler)
* [Web Services](#web-services)
  * [Topology](#topology)
  * [Interactive Queries](#interactive-queries)
  * [Kubernetes](#kubernetes)
* [TopicWithSerde API](#topicwithserde-api)
  * [Declaration](#declaration)
  * [Prefix](#prefix)
  * [Remapping](#remapping)
  * [Unit Test](#unit-test-1)
* [Interactive Queries](#interactive-queries-1)
  * [Configuration](#configuration)
  * [Services](#services)
  * [Web Services](#web-services-1)
* [Hooks](#hooks)
  * [On Start](#on-start)
* [Utils](#utils)
  * [KafkaStreams Execution Context](#kafkastreams-execution-context)
  * [Topic](#topic)
  * [Deduplication](#deduplication)
    * [By Key](#by-key)
    * [By Key and Value](#by-key-and-value)
    * [By Predicate](#by-predicate)
* [Open Telemetry](#open-telemetry)
  * [Custom Tags for Metrics](#custom-tags-for-metrics)
* [Swagger](#swagger)
* [Motivation](#motivation)
* [Contribution](#contribution)

## Overview

Wondering what makes Kstreamplify stand out? Here are some of the key features that make it a must-have for Kafka Streams:

- **🚀 Bootstrapping**: Automatically handles the startup, configuration, and initialization of Kafka Streams so you can focus on business logic instead of setup.

- **📝 Avro Serializer and Deserializer**: Provides common Avro serializers and deserializers out of the box.

- **⛑️ Error Handling**: Catches and routes errors to a dead-letter queue (DLQ) topic.

- **☸️ Kubernetes**: Built-in readiness and liveness probes for Kubernetes deployments.

- **🤿 Interactive Queries**: Easily access and interact with Kafka Streams state stores.

- **🫧 Deduplication**: Remove duplicate events from your stream.

- **🧪 Testing**: Automatically sets up the Topology Test Driver so you can start writing tests right away.

## Getting Started

### Spring Boot

For Spring applications using the Spring Boot starter parent:

```xml
<dependency>
    <groupId>com.michelin</groupId>
    <artifactId>kstreamplify-spring-boot</artifactId>
    <version>${kstreamplify.version}</version>
</dependency>
```

Create a `KafkaStreamsStarter` bean and override the `KafkaStreamsStarter#topology()` method:

```java
@Component
public class MyKafkaStreams extends KafkaStreamsStarter {
    @Override
    public void topology(StreamsBuilder streamsBuilder) {
        // Define your topology here
    }

    @Override
    public String dlqTopic() {
        return "dlq_topic";
    }
}
```

Define Kafka Streams properties in `application.yml` under the `kafka.properties` key:

```yml
kafka:
  properties:
    application.id: 'myKafkaStreams'
    bootstrap.servers: 'localhost:9092'
    schema.registry.url: 'http://localhost:8081'
```

You're now ready to start your Kstreamplify Spring Boot application.

### Java

For framework-independent Java applications:

```xml
<dependency>
    <groupId>com.michelin</groupId>
    <artifactId>kstreamplify-core</artifactId>
    <version>${kstreamplify.version}</version>
</dependency>
```

Create a class that extends `KafkaStreamsStarter` and override the `KafkaStreamsStarter#topology()` method:

```java
public class MyKafkaStreams extends KafkaStreamsStarter {
    @Override
    public void topology(StreamsBuilder streamsBuilder) {
        // Define your topology here
    }

    @Override
    public String dlqTopic() {
        return "dlq_topic";
    }
}
```

From your `main` method, initialize `KafkaStreamsInitializer` with your `KafkaStreamsStarter` implementation:

```java
public class MainKstreamplify {

    public static void main(String[] args) {
        KafkaStreamsInitializer initializer = new KafkaStreamsInitializer(new MyKafkaStreams());
        initializer.start();
    }
}
```

Define Kafka Streams properties in `src/main/resources/application.yml` under the `kafka.properties` key:

```yml
kafka:
  properties:
    application.id: 'myKafkaStreams'
    bootstrap.servers: 'localhost:9092'
    schema.registry.url: 'http://localhost:8081'
server:
  port: 8080
```

You're now ready to start your Kstreamplify Java application.

**Notes**:
- `server.port` is required to enable the [web services](#web-services).
- The core dependency does not include a logger. Add one to your project.

### Unit Test

Kstreamplify simplifies testing Kafka Streams applications using the **Topology Test Driver**.

Add the test dependency for both Java and Spring Boot applications:

```xml
<dependency>
    <groupId>com.michelin</groupId>
    <artifactId>kstreamplify-core-test</artifactId>
    <version>${kstreamplify.version}</version>
    <scope>test</scope>
</dependency>
```

Create a test class extending `KafkaStreamsStarterTest` and override `getKafkaStreamsStarter()`:

```java
public class MyKafkaStreamsTest extends KafkaStreamsStarterTest {
    private TestInputTopic<String, KafkaUser> inputTopic;
    private TestOutputTopic<String, KafkaUser> outputTopic;

    @Override
    protected KafkaStreamsStarter getKafkaStreamsStarter() {
        return new MyKafkaStreams();
    }

    @BeforeEach
    void setUp() {
        inputTopic = testDriver.createInputTopic("input_topic", new StringSerializer(),
            SerdesUtils.<KafkaUser>getValueSerdes().serializer());

        outputTopic = testDriver.createOutputTopic("output_topic", new StringDeserializer(),
            SerdesUtils.<KafkaUser>getValueSerdes().deserializer());
    }

    @Test
    void shouldUpperCase() {
        inputTopic.pipeInput("1", user);
        List<KeyValue<String, KafkaUser>> results = outputTopic.readKeyValuesToList();
        assertEquals("FIRST NAME", results.get(0).value.getFirstName());
        assertEquals("LAST NAME", results.get(0).value.getLastName());
    }

    @Test
    void shouldFailAndRouteToDlqTopic() {
        inputTopic.pipeInput("1", user);
        List<KeyValue<String, KafkaError>> errors = dlqTopic.readKeyValuesToList();
        assertEquals("1", errors.get(0).key);
        assertEquals("Something bad happened...", errors.get(0).value.getContextMessage());
        assertEquals(0, errors.get(0).value.getOffset());
    }
}
```

#### Override Properties

Kstreamplify uses default properties in tests. Override or add properties by overriding `getSpecificProperties()`:

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

## Avro Serializer and Deserializer

When working with Avro schemas, you can use the `SerdesUtils` class to easily serialize or deserialize records:

```java
SerdesUtils.<MyAvroValue>getValueSerdes()
```

or

```java
SerdesUtils.<MyAvroValue>getKeySerdes()
```

Here's an example of using these methods in your topology:

```java
@Component
public class MyKafkaStreams extends KafkaStreamsStarter {
    @Override
    public void topology(StreamsBuilder streamsBuilder) {
        streamsBuilder
            .stream("input_topic", Consumed.with(Serdes.String(), SerdesUtils.<KafkaUser>getValueSerdes()))
            .to("output_topic", Produced.with(Serdes.String(), SerdesUtils.<KafkaUser>getValueSerdes()));
    }
}
```

## Error Handling

Kstreamplify makes it easy to handle errors and route them to a dead-letter queue (DLQ) topic.

### Set up DLQ Topic

Override the `dlqTopic()` method and return the name of your DLQ topic:

```java
@Component
public class MyKafkaStreams extends KafkaStreamsStarter {
    @Override
    public void topology(StreamsBuilder streamsBuilder) {
        // Define your topology here
    }

    @Override
    public String dlqTopic() {
        return "dlq_topic";
    }
}
```

### Handling Processing Errors

To catch processing errors and route them to the DLQ, use the `ProcessingResult` class.

#### DSL

```java
@Component
public class MyKafkaStreams extends KafkaStreamsStarter {
    @Override
    public void topology(StreamsBuilder streamsBuilder) {
        KStream<String, KafkaUser> stream = streamsBuilder
            .stream("input_topic", Consumed.with(Serdes.String(), SerdesUtils.getValueSerdes()));

        TopologyErrorHandler
            .catchErrors(stream.mapValues(MyKafkaStreams::toUpperCase))
            .to("output_topic", Produced.with(Serdes.String(), SerdesUtils.getValueSerdes()));
    }

    @Override
    public String dlqTopic() {
        return "dlq_topic";
    }

    private static ProcessingResult<KafkaUser, KafkaUser> toUpperCase(KafkaUser value) {
        try {
            value.setLastName(value.getLastName().toUpperCase());
            return ProcessingResult.success(value);
        } catch (Exception e) {
            return ProcessingResult.fail(e, value, "Something went wrong...");
        }
    }
}
```

The `mapValues` operation returns a `ProcessingResult<V, V2>`, where:

- The first type parameter (`V`) represents the transformed value upon success.
- The second type (`V2`) represents the original value if an error occurs.

To mark a result as successful:

```java
ProcessingResult.success(value);
```

To mark it as failed:

```java
ProcessingResult.fail(e, value, "Something went wrong...");
```

Use `TopologyErrorHandler#catchErrors()` to catch and route failed records to the DLQ topic. A healthy stream is returned and can be further processed as needed.

#### Processor API

```java
@Component
public class MyKafkaStreams extends KafkaStreamsStarter {
    @Override
    public void topology(StreamsBuilder streamsBuilder) {
        TopologyErrorHandler.catchErrors(
            streamsBuilder.stream("input_topic", Consumed.with(Serdes.String(), Serdes.String()))
                .process(CustomProcessor::new)
            )
            .to("output_topic", Produced.with(Serdes.String(), Serdes.String()));
    }

    @Override
    public String dlqTopic() {
        return "dlq_topic";
    }

    public static class CustomProcessor extends ContextualProcessor<String, String, String, ProcessingResult<String, String>> {
        @Override
        public void process(Record<String, String> record) {
            try {
              context().forward(ProcessingResult.wrapRecordSuccess(record.withValue(record.value().toUpperCase())));
            } catch (Exception e) {
              context().forward(ProcessingResult.wrapRecordFailure(e, record.withValue(record.value()), "Something went wrong..."));
            }
        }
    }
}
```

The `process` operation forwards a `ProcessingResult<V, V2>`, where:

- The first type parameter (`V`) represents the transformed value upon success.
- The second type (`V2`) represents the original value if an error occurs.

To mark a result as successful:

```java
ProcessingResult.wrapRecordSuccess(record);
```

To mark it as failed:

```java
ProcessingResult.wrapRecordFailure(e, record, "Something went wrong...");
```

Use `TopologyErrorHandler#catchErrors()` to catch and route failed records to the DLQ topic. A healthy stream is returned and can be further processed as needed.

### Production and Deserialization Errors

Kstreamplify provides handlers implementations to forward production and deserialization errors to the DLQ.

Add the following properties to your `application.yml`:

```yml
kafka:
  properties:
    deserialization.exception.handler: 'com.michelin.kstreamplify.error.DlqDeserializationExceptionHandler'
    production.exception.handler: 'com.michelin.kstreamplify.error.DlqProductionExceptionHandler'
```

Additionally, some exceptions can optionally be forwarded to the DLQ by enabling the following properties:

```yml
kafka:
  properties:
    dlq:
      deserialization-handler:
        forward-restclient-exception: true
```

| Property                                                   | Handler                           | Description                                                                                |
|------------------------------------------------------------|-----------------------------------|--------------------------------------------------------------------------------------------|
| `dlq.deserialization-handler.forward-restclient-exception` | Deserialization Exception Handler | Forwards `RestClientException` from the Schema Registry (e.g., when a schema is not found) |

### Avro Schema

The DLQ topic must have an associated Avro schema registered in the Schema Registry.
You can find the schema [here](https://github.com/michelin/kstreamplify/blob/main/kstreamplify-core/src/main/avro/kafka-error.avsc).

### Uncaught Exception Handler

By default, uncaught exceptions will shut down the Kafka Streams client.

To customize this behavior, override the `KafkaStreamsStarter#uncaughtExceptionHandler()` method:

```java
@Override
public StreamsUncaughtExceptionHandler uncaughtExceptionHandler() {
    return throwable -> StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_APPLICATION;
}
```

## Web Services

Kstreamplify exposes web services on top of your Kafka Streams application.

### Topology

The `/topology` endpoint returns the Kafka Streams topology description by default.
You can customize the path by setting the following property:

```yml
topology:
  path: 'custom-topology'
```

### Interactive Queries

A set of endpoints is available to query the state stores of your Kafka Streams application.
These endpoints leverage [interactive queries](https://docs.confluent.io/platform/current/streams/developer-guide/interactive-queries.html) and handle state stores across different Kafka Streams instances by providing an [RPC layer](https://docs.confluent.io/platform/current/streams/developer-guide/interactive-queries.html#adding-an-rpc-layer-to-your-application).

The following state store types are supported:
- Key-Value store
- Timestamped Key-Value store
- Window store
- Timestamped Window store

Note that only state stores with String keys are supported.

### Kubernetes

Readiness and liveness probes are exposed for Kubernetes deployment, reflecting the Kafka Streams state.
These are available at `/ready` and `/liveness` by default.
You can customize the paths by setting the following properties:

```yml
kubernetes:
  liveness:
    path: 'custom-liveness'
  readiness:
    path: 'custom-readiness'
```

## TopicWithSerde API

Kstreamplify provides an API called `TopicWithSerde` that unifies all consumption and production points, simplifying the management of topics owned by different teams across multiple environments.

### Declaration

You can declare your consumption and production points in a separate class. This requires a topic name, a key SerDe, and a value SerDe.

```java
public static TopicWithSerde<String, KafkaUser> inputTopic() {
    return new TopicWithSerde<>(
        "input_topic",
        Serdes.String(),
        SerdesUtils.getValueSerdes()
    );
}

public static TopicWithSerde<String, KafkaUser> outputTopic() {
    return new TopicWithSerde<>(
        "output_topic",
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
        KStream<String, KafkaUser> stream = inputTopic().stream(streamsBuilder);
        outputTopic().produce(stream);
    }
}
```

### Prefix

The `TopicWithSerde` API is designed to handle topics owned by different teams across various environments without changing the topology. It uses prefixes to differentiate teams and topic ownership.

In your `application.yml` file, declare the prefixes in a `key: value` format:

```yml
kafka:
  properties:
    prefix:
      self: 'staging.team1.'
      team2: 'staging.team2.'
      team3: 'staging.team3.'
```

Then, include the prefix when declaring your `TopicWithSerde`:

```java
public static TopicWithSerde<String, KafkaUser> inputTopic() {
    return new TopicWithSerde<>(
        "input_topic",
        "team1",
        Serdes.String(),
        SerdesUtils.getValueSerdes()
    );
}
```

> The topic `staging.team1.input_topic` will be consumed when running the application with the staging `application.yml` file.

By default, if no prefix is specified, `self` is used.

### Remapping

Kstreamplify encourages the use of fixed topic names in the topology, using the prefix feature to manage namespacing for virtual clusters and permissions. 
However, there are situations where you might want to reuse the same topology with different input or output topics.

In the `application.yml` file, you can declare dynamic remappings in a `key: value` format:

```yml
kafka:
  properties:
    topic:
      remap:
        oldTopicName: newTopicName
        foo: bar
```

> The topic `oldTopicName` in the topology will be mapped to `newTopicName`.

This feature works with both input and output topics.

### Unit Test

When testing, you can use the `TopicWithSerde` API to create test topics with the same name as those in your topology.

```java
TestInputTopic<String, KafkaUser> inputTopic = createInputTestTopic(inputTopic());
TestInputTopic<String, KafkaUser> outputTopic = createOutputTestTopic(outputTopic());
```

## Interactive Queries

Kstreamplify aims to simplify the use of [interactive queries](https://docs.confluent.io/platform/current/streams/developer-guide/interactive-queries.html) in Kafka Streams application.

### Configuration

The value for the "[application.server](https://docs.confluent.io/platform/current/streams/developer-guide/config-streams.html#application-server)" property can be derived from various sources, following this order of priority:

1. The environment variable defined by the `application.server.var.name` property.

```yml
kafka:
  properties:
    application.server.var.name: 'MY_APPLICATION_SERVER'
```

2. If not defined, it defaults to the `APPLICATION_SERVER` environment variable.
3. If neither of the above is set, it defaults to `localhost:<serverPort>`.

### Services

You can leverage the interactive query services provided by the web services layer to access and query the state stores of your Kafka Streams application:

```java
@Component
public class MyService {
    @Autowired
    KeyValueStoreService keyValueStoreService;

    @Autowired
    TimestampedKeyValueStoreService timestampedKeyValueStoreService;
  
    @Autowired
    WindowStoreService windowStoreService;

    @Autowired
    TimestampedWindowStoreService timestampedWindowStoreService;
}
```

### Web Services

The web services layer provides a set of endpoints that allow you to query the state stores of your Kafka Streams application. You can find more details in the [Interactive Queries Web Services](#interactive-queries) section.

## Hooks

Kstreamplify provides the flexibility to execute custom code through hooks at various stages of your Kafka Streams application lifecycle.

### On Start

The **On Start** hook allows you to execute custom code before the Kafka Streams instance starts.

```java
@Component
public class MyKafkaStreams extends KafkaStreamsStarter {
    @Override
    public void onStart(KafkaStreams kafkaStreams) {
        // Execute code before starting the Kafka Streams instance
    }
}
```

## Utils

Here is the list of utils available in Kstreamplify.

### KafkaStreams Execution Context

The `KafkaStreamsExecutionContext` provides static access to several pieces of information:

- The DLQ topic name
- The SerDes configuration
- The Kafka properties
- The instance's own prefix (used to prefix the `application.id`)

### Topic

The `TopicUtils` class provides a utility method to prefix any topic name with a custom prefix defined in the `application.yml` file.

```java
TopicUtils.remapAndPrefix("input_topic", "team3");
```

For more details about prefixes, see the [Prefix](#prefix) section.

### Deduplication

The `DeduplicationUtils` class helps you deduplicate streams based on various criteria and within a specified time window.

All deduplication methods return a `KStream<String, ProcessingResult<V,V2>`, allowing you to handle errors and route them to `TopologyErrorHandler#catchErrors()`.

**Note**: Only streams with `String` keys and Avro values are supported.

#### By Key

```java
@Component
public class MyKafkaStreams extends KafkaStreamsStarter {
    @Override
    public void topology(StreamsBuilder streamsBuilder) {
        KStream<String, KafkaUser> myStream = streamsBuilder
            .stream("input_topic");

        DeduplicationUtils
            .deduplicateKeys(streamsBuilder, myStream, Duration.ofDays(60))
            .to("output_topic");
    }
}
```

#### By Key and Value

```java
@Component
public class MyKafkaStreams extends KafkaStreamsStarter {
    @Override
    public void topology(StreamsBuilder streamsBuilder) {
        KStream<String, KafkaUser> myStream = streamsBuilder
            .stream("input_topic");

        DeduplicationUtils
            .deduplicateKeyValues(streamsBuilder, myStream, Duration.ofDays(60))
            .to("output_topic");
    }
}
```

#### By Predicate

```java
@Component
public class MyKafkaStreams extends KafkaStreamsStarter {
    @Override
    public void topology(StreamsBuilder streamsBuilder) {
        KStream<String, KafkaUser> myStream = streamsBuilder
            .stream("input_topic");

        DeduplicationUtils
            .deduplicateWithPredicate(streamsBuilder, myStream, Duration.ofDays(60),
                value -> value.getFirstName() + "#" + value.getLastName())
            .to("output_topic");
    }
}
```

In the predicate approach, the provided predicate is used as the key in the window store. The stream will be deduplicated based on the values derived from the predicate.

## Open Telemetry

Kstreamplify simplifies the integration of [Open Telemetry](https://opentelemetry.io/) with Kafka Streams applications in Spring Boot.
It binds all Kafka Streams metrics to the Spring Boot registry, making monitoring and observability easier.

To run your application with the Open Telemetry Java agent, include the following JVM options:

```console
-javaagent:/opentelemetry-javaagent.jar -Dotel.traces.exporter=otlp -Dotel.logs.exporter=otlp -Dotel.metrics.exporter=otlp
```

Starting with OpenTelemetry Java Agent version 2, the Micrometer metrics bridge has been disabled (see [release notes](https://github.com/open-telemetry/opentelemetry-java-instrumentation/releases/tag/v2.0.0)).
You need to manually enable it by adding the following option: `-Dotel.instrumentation.micrometer.enabled=true`.

### Custom Tags for Metrics

You can also add custom tags to the Open Telemetry metrics to help organize and filter them in your observability tools, like Grafana. 
Use the following JVM options to specify custom tags:

```console
-Dotel.resource.attributes=environment=production,service.namespace=myNamespace,service.name=myKafkaStreams,category=orders
```

These tags will be included in the metrics, and you'll be able to see them in your logs during application startup, helping to track and filter metrics based on attributes like environment, service name, and category.

## Swagger

The Kstreamplify Spring Boot module integrates with [Springdoc](https://springdoc.org/) to automatically generate API documentation for your Kafka Streams application.

By default:
- The Swagger UI is available at `http://host:port/swagger-ui/index.html`.
- The OpenAPI documentation can be accessed at `http://host:port/v3/api-docs`.

Both the Swagger UI and the OpenAPI description can be customized using the [Springdoc properties](https://springdoc.org/#properties).

## Motivation

Developing applications with Kafka Streams can be challenging, with developers often facing various questions and obstacles. 
Key considerations include efficiently bootstrapping Kafka Streams applications, handling unexpected business logic issues, integrating Kubernetes probes, and more.

To assist developers in overcoming these challenges, we have built **Kstreamplify**.
Our goal is to provide a comprehensive solution that simplifies the development process and addresses the common pain points encountered when working with Kafka Streams.
By offering easy-to-use utilities, error handling mechanisms, testing support, and integration with modern tools like Kubernetes and OpenTelemetry, Kstreamplify aims to streamline Kafka Streams application development.

## Contribution

We welcome contributions from the community! Before you get started, please take a look at
our [contribution guide](https://github.com/michelin/kstreamplify/blob/main/CONTRIBUTING.md) to learn about our guidelines
and best practices. We appreciate your help in making Kstreamplify a better tool for everyone.
