# Kstreamplify

[![GitHub Build](https://img.shields.io/github/actions/workflow/status/michelin/kstreamplify/on_push_main.yml?branch=main&logo=github&style=for-the-badge)](https://img.shields.io/github/actions/workflow/status/michelin/kstreamplify/on_push_main.yml)
[![Sonatype Nexus (Releases)](https://img.shields.io/nexus/r/com.michelin/kstreamplify?server=https%3A%2F%2Fs01.oss.sonatype.org%2F&style=for-the-badge&logo=sonatype)](https://central.sonatype.com/search?q=com.michelin.kstreamplify&sort=name)
[![GitHub release](https://img.shields.io/github/v/release/michelin/kstreamplify?logo=github&style=for-the-badge)](https://github.com/michelin/kstreamplify/releases)
[![GitHub commits since latest release (by SemVer)](https://img.shields.io/github/commits-since/michelin/kstreamplify/latest?logo=github&style=for-the-badge)](https://github.com/michelin/kstreamplify/commits/main)
[![GitHub Stars](https://img.shields.io/github/stars/michelin/kstreamplify?logo=github&style=for-the-badge)](https://github.com/michelin/kstreamplify)
[![GitHub Watch](https://img.shields.io/github/watchers/michelin/kstreamplify?logo=github&style=for-the-badge)](https://github.com/michelin/kstreamplify)
[![SonarCloud Coverage](https://img.shields.io/sonar/coverage/michelin_kstreamplify?logo=sonarcloud&server=https%3A%2F%2Fsonarcloud.io&style=for-the-badge)](https://sonarcloud.io/component_measures?id=michelin_kstreamplify&metric=coverage&view=list)
[![SonarCloud Tests](https://img.shields.io/sonar/tests/michelin_kstreamplify/main?server=https%3A%2F%2Fsonarcloud.io&style=for-the-badge&logo=sonarcloud)](https://sonarcloud.io/component_measures?metric=tests&view=list&id=michelin_kstreamplify)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg?logo=apache&style=for-the-badge)](https://opensource.org/licenses/Apache-2.0)

Are you looking to enhance your development experience and accelerate the implementation of Kafka Streams? Look no
further – Kstreamplify is tailor-made for you!

**Kstreamplify** is a Java library that empowers you to swiftly create Kafka Streams-based applications, offering a host
of additional advanced features.

With Kstreamplify, you can declare your KafkaStreams class and define your topology with minimal effort. Here's all you
need to do:

<img src=".readme/gif/topology.gif" alt="topology"/>

## Table of Contents

* [Overview](#overview)
* [Dependencies](#dependencies)
    * [Java](#java)
    * [Spring Boot](#spring-boot)
    * [Unit Test](#unit-test)
* [Features](#features)
  * [Bootstrapping](#bootstrapping)
    * [Create your first Kstreamplify application](#create-your-first-kstreamplify-application) 
    * [Properties Injection](#properties-injection)
  * [Avro Serializer and Deserializer](#avro-serializer-and-deserializer)
  * [Error Handling](#error-handling)
    * [Topology](#topology)
    * [Production and Deserialization](#production-and-deserialization)
    * [Avro Schema](#avro-schema)
    * [Uncaught Exception Handler](#uncaught-exception-handler)
  * [Kubernetes](#kubernetes)
  * [Hooks](#hooks)
    * [On Start](#on-start)
  * [Interactive Queries](#interactive-queries)
    * [Application Server Configuration](#application-server-configuration)
    * [Querying State Stores with REST Endpoints](#querying-state-stores-with-rest-endpoints)
  * [Topology](#topology-2)
  * [Deduplication](#deduplication)
    * [By Key](#by-key)
    * [By Key and Value](#by-key-and-value)
    * [By Predicate](#by-predicate)
  * [Open Telemetry](#open-telemetry)
  * [Testing](#testing)
* [Motivation](#motivation)
* [Contribution](#contribution)

## Overview

Wondering what makes Kstreamplify stand out? Here are some of the key features that make it a must-have for Kafka Streams:

- **🚀 Bootstrapping**: Automatic startup, configuration, and initialization of Kafka Streams is handled for you. Focus on
business implementation rather than the setup.

- **📝 Avro Serializer and Deserializer**: Common serializers and deserializers for Avro.

- **⛑️ Error Handling**: Catch and route errors to a dead-letter queue (DLQ) topic

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

If you are using Spring Boot, you can integrate Kstreamplify with your Spring Boot application by adding the following
dependency:

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

## Features

Kstreamplify offers a wide range of features to simplify the development of Kafka Streams applications.

### Bootstrapping

Kstreamplify simplifies the bootstrapping of Kafka Streams applications by handling the startup, configuration, and
initialization of Kafka Streams for you.

#### Create your first Kstreamplify application

To create a Kstreamplify application, define a `KafkaStreamsStarter` bean within your Spring Boot context and
override the `KafkaStreamsStarter#topology()` method:

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

#### Properties Injection

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

Note that all the Kafka Streams properties have been moved under `kafka.properties`.

### Avro Serializer and Deserializer

Whenever you need to serialize or deserialize records with Avro schemas, you can use the `SerdeUtils` class as follows:

```java
SerdeUtils.<MyAvroValue>getValueSerde()
```

or

```java
SerdeUtils.<MyAvroValue>getKeySerde()
```

Here is an example of using these methods in your topology:

```java
@Component
public class MyKafkaStreams extends KafkaStreamsStarter {
    @Override
    public void topology(StreamsBuilder streamsBuilder) {
        streamsBuilder
            .stream("INPUT_TOPIC", Consumed.with(Serdes.String(), SerdeUtils.<KafkaPerson>getValueSerde()))
            .to("OUTPUT_TOPIC", Produced.with(Serdes.String(), SerdeUtils.<KafkaPerson>getValueSerde()));
    }
}
```

### Error Handling

Kstreamplify provides the ability to handle errors that may occur in your topology as well as during the production or
deserialization of records and route them to a dead-letter queue (DLQ) topic.

To do it, start by overriding the `dlqTopic` method and return the name of your DLQ topic:

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

#### Topology

Kstreamplify provides utilities to handle errors that occur in your topology and route them to a DLQ topic
automatically.

The processing result is encapsulated and marked as either success or failure.
Failed records will be routed to the DLQ topic, while successful records will still be up for further processing.

```java
@Component
public class MyKafkaStreams extends KafkaStreamsStarter {
    @Override
    public void topology(StreamsBuilder streamsBuilder) {
        KStream<String, KafkaPerson> stream = streamsBuilder
            .stream("INPUT_TOPIC", Consumed.with(Serdes.String(), SerdeUtils.getValueSerde()));

        TopologyErrorHandler
            .catchErrors(stream.mapValues(MyKafkaStreams::toUpperCase))
            .to("OUTPUT_TOPIC", Produced.with(Serdes.String(), SerdeUtils.getValueSerde()));
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

You can use the following to mark the result as successful:

```java
ProcessingResult.success(value);
```

Or the following in a catch clause to mark the result as failed:

```java
ProcessingResult.fail(e, value, "Something bad happened...");
```

The stream of `ProcessingResult<V,V2>` needs to be lightened of the failed records by sending them to the DLQ topic.
This is done by invoking the `TopologyErrorHandler#catchErrors()` method.
A healthy stream is then returned and can be further processed.

#### Production and Deserialization

Kstreamplify provides production and deserialization handlers that send errors to the DLQ topic.

```yml
kafka:
  properties:
    default.production.exception.handler: com.michelin.kstreamplify.error.DlqProductionExceptionHandler
    default.deserialization.exception.handler: com.michelin.kstreamplify.error.DlqDeserializationExceptionHandler
```

#### Avro Schema

An Avro schema needs to be deployed in a Schema Registry on top of the DLQ topic. It is
available [here](https://github.com/michelin/kstreamplify/blob/main/kstreamplify-core/src/main/avro/kafka-error.avsc).

#### Uncaught Exception Handler

Kstreamplify defines a default uncaught exception handler that catches all uncaught exceptions and shuts down the client.

If you want to override this behavior, you can override the `KafkaStreamsStarter#uncaughtExceptionHandler()` method and return your own 
uncaught exception handler.

```java
@Override
public StreamsUncaughtExceptionHandler uncaughtExceptionHandler() {
    return throwable -> {
        return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_APPLICATION;
    };
}
```

### Kubernetes

Kstreamplify provides readiness and liveness probes for Kubernetes deployment based on the Kafka Streams state.

By default, the endpoints are available at `/ready` and `/liveness`.

The path can be customized by setting the following properties:

```yml
kubernetes:
  readiness:
    path: custom-readiness
  liveness:
    path: custom-liveness
```

### Hooks

Kstreamplify offers the flexibility to execute custom code through hooks.

#### On Start

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

### Interactive Queries

Kstreamplify wants to ease the use of [interactive queries](https://docs.confluent.io/platform/current/streams/developer-guide/interactive-queries.html) in Kafka Streams application.

#### Application Server Configuration

The "[application.server](https://docs.confluent.io/platform/current/streams/developer-guide/config-streams.html#application-server)" property value is determined from different sources by the following order of priority:

1. The value of an environment variable whose name is defined by the `application.server.var.name` property.

```yml
kafka:
  properties:
    application.server.var.name: MY_APPLICATION_SERVER
```

2. The value of a default environment variable named `APPLICATION_SERVER`.
3. `localhost`.

#### Querying State Stores with REST Endpoints

Kstreamplify provides REST endpoints to query the state stores of your Kafka Streams application.
It handles state stores being on different Kafka Streams instances by providing an [RPC layer](https://docs.confluent.io/platform/current/streams/developer-guide/interactive-queries.html#adding-an-rpc-layer-to-your-application).

Here is an overview of the available endpoints:

- GET `/store`: Returns the list of available state stores
- GET `/store/{store}/info`: Returns the host information of the state store
- GET `/store/{store}`: Returns all records of the state store
- GET `/store/{store}/{key}`: Returns the record of the state store for the given key

<h3 id="topology-2">Topology</h4>

Kstreamplify provides a REST endpoint to retrieve the Kafka Streams topology as JSON.

By default, the endpoint is available at `/topology`.

The path can be customized by setting the following properties:

```yml
topology:
  path: custom-topology
```

### Deduplication

Kstreamplify facilitates deduplication of a stream through the `DeduplicationUtils` class, based on various criteria
and within a specified time frame.

All deduplication methods return a `KStream<String, ProcessingResult<V,V2>` so you can redirect the result to the
`TopologyErrorHandler#catchErrors()`.

**Note**: Only streams with String keys and Avro values are supported.

#### By Key

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

#### By Key and Value

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

#### By Predicate

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

### Open Telemetry

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

### Testing

Kstreamplify eases the use of the Topology Test Driver for testing Kafka Streams application.

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
            SerdeUtils.<KafkaPerson>getValueSerde().serializer());

        outputTopic = testDriver.createOutputTopic("OUTPUT_TOPIC", new StringDeserializer(),
            SerdeUtils.<KafkaPerson>getValueSerde().deserializer());
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
