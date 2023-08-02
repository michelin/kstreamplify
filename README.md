<h1 align="center"> Kstreamplify </h1>

[![GitHub Build](https://img.shields.io/github/actions/workflow/status/michelin/kstreamplify/on_push_main.yml?branch=main&logo=github&style=for-the-badge)](https://img.shields.io/github/actions/workflow/status/michelin/kstreamplify/on_push_main.yml)
[![GitHub Stars](https://img.shields.io/github/stars/michelin/kstreamplify?logo=github&style=for-the-badge)](https://github.com/michelin/kstreamplify)
[![GitHub Watch](https://img.shields.io/github/watchers/michelin/kstreamplify?logo=github&style=for-the-badge)](https://github.com/michelin/kstreamplify)
[![SonarCloud Coverage](https://img.shields.io/sonar/coverage/michelin_kstreamplify?logo=sonarcloud&server=https%3A%2F%2Fsonarcloud.io&style=for-the-badge)](https://sonarcloud.io/component_measures?id=michelin_kstreamplify&metric=coverage&view=list)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg?logo=apache&style=for-the-badge)](https://opensource.org/licenses/Apache-2.0)

Kstreamplify is a Java library that brings new features on top of Kafka Streams.

## Table of Contents

* [Features](#features)
* [Dependencies](#dependencies)
* [Getting Started](#getting-started)
  * [Properties Injection](#properties-injection)
  * [Avro Serializer and Deserializer](#avro-serializer-and-deserializer)
  * [Error Handling](#error-handling)
    * [Topology](#topology)
    * [Production and Deserialization](#production-and-deserialization)
    * [Avro Schema](#avro-schema)
  * [REST Endpoints](#rest-endpoints)
  * [Testing](#testing)
* [Motivation](#motivation)
* [Contribution](#contribution)

## Features

- **Easy bootstrapping**: Kafka Streams application bootstrapping is handled for you, allowing you to focus on topology implementation.

- **Avro Schema Serializer and Deserializer**: Common serializers and deserializers for all your Avro specific records.

- **Error Handling**: A strong error handling mechanism is provided for topology, production, and deserialization errors, and it also allows routing them into a dead letter queue (DLQ) topic.

- **REST endpoints**: Some useful REST endpoints, including Kubernetes liveness and readiness probes. 

- **Testing**: The library eases the use of Topology Test Driver, making it easier to write your tests.

## Dependencies

Kstreamplify provides three dependencies.

### Java SE

These dependencies are compatible with Java Standard Edition (SE).

```xml
<dependency>
  <groupId>com.michelin</groupId>
  <artifactId>kstreamplify-core</artifactId>
  <version>${kstreamplify.version}</version>
</dependency>

<dependency>
    <groupId>com.michelin</groupId>
    <artifactId>kstreamplify-core-test</artifactId>
    <version>${kstreamplify.version}</version>
    <scope>test</scope>
</dependency>
```

### Spring Boot

If you are using Spring Boot, you can use the following dependency to integrate Kstreamplify with your Spring Boot application:

```xml
<dependency>
    <groupId>com.michelin</groupId>
    <artifactId>kstreamplify-spring-boot</artifactId>
    <version>${kstreamplify.version}</version>
</dependency>
```

The dependency is compatible with Spring Boot 3.

## Getting Started

To begin using Kstreamplify, you simply need to set up a `KafkaStreamsStarter` bean within you Spring Boot context, overriding the `topology` method. 

For instance, you can start by creating a class annotated with `@Component`:

```java
@Component
public class MyKafkaStreams implements KafkaStreamsStarter {
    @Override
    public void topology(StreamsBuilder streamsBuilder) { 
        // Your topology here
    }
}
```

Alternatively, you can annotate a method that returns a `KafkaStreamsStarter` with `@Bean`:

```java
@Bean
public KafkaStreamsStarter kafkaStreamsStarter() {
    return streamsBuilder -> streamsBuilder.map(...).to(...); // Your topology here
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

Note that all the properties have been moved under `kafka.properties`.

### Avro Serializer and Deserializer

Whenever you need to serialize or deserialize records with Avro schemas, you can use the `SerdesUtils` class as follows:

```java
SerdesUtils.<MyAvroValue>getSerdesForValue()
```

or 

```java
SerdesUtils.<MyAvroValue>getSerdesForKey()
```

Here's an example of using these methods in your topology:

```java
@Override
public void topology(StreamsBuilder streamsBuilder) {
    streamsBuilder
            .stream("inputTopic", Consumed.with(Serdes.String(), SerdesUtils.<MyAvroValue>getSerdesForValue()))
            // ...
            .to("outputTopic", Produced.with(Serdes.String(), SerdesUtils.<MyAvroValue>getSerdesForValue()));
}
```

### Error Handling

The library provides the ability to handle errors that may occur in your topology as well as during the production or deserialization of records and route them to a dead-letter queue (DLQ) topic.

To do this, the first step is to override the `dlqTopic` method and return the name of your DLQ topic:

```java
@Component
public class MyKafkaStreams implements KafkaStreamsStarter {
    @Override
    public void topology(StreamsBuilder streamsBuilder) { //... }
    
    @Override
    public String dlqTopic() {
        return "myDlqTopic";
    }
}
```

#### Topology 

Kstreamplify provides utilities to handle all the unexpected errors that can occur in your topologies and route them to a dead-letter queue (DLQ) topic automatically.

The principle is simple: whenever you perform transformations on stream values, you can encapsulate the result as either success or failure. Failed records will be routed to your DLQ topic, while successful records will still be up for further processing.

Here is a complete example of how to do this:

```java
@Component
public class MyKafkaStreams implements KafkaStreamsStarter {
    @Override
    public void topology(StreamsBuilder streamsBuilder) {
        KStream<String, MyAvroValue> myStream = streamsBuilder
                .stream("inputTopic",
                        Consumed.with(Serdes.String(), SerdesUtils.<MyAvroValue>getSerdesForValue()));

        TopologyErrorHandler
                .catchErrors(myStream.mapValues(MyKafkaStreams::toUpperCase))
                .to("outputTopic",
                        Produced.with(Serdes.String(), SerdesUtils.<MyAvroValue>getSerdesForValue()));
    }

    @Override
    public String dlqTopic() {
        return "myDlqTopic";
    }

    private static ProcessingResult<MyAvroValue, MyAvroValue> toUpperCase(MyAvroValue value) {
        try {
            value.setValue(value.getValue().toUpperCase());
            return ProcessingResult.success(value);
        } catch (Exception ex) {
            return ProcessingResult.fail(ex, value, "An error occurred during the upper case map values process");
        }
    }
}
```

The first step is during the map values processing. The operation should return a new value of type `ProcessingResult<V, V2>`.
The first templatized parameter is the type of the new value after a successful transformation.
The second templatized parameter is the type of the current value for which the transformation failed.

You can use the following to mark the result as successful:

```java
return ProcessingResult.success(value);
```

Or the following in a catch clause to mark the result as failed:

```java
return ProcessingResult.fail(ex, value, "An error occurred during the upper case map values process");
```

The `ProcessingResult.fail()` method takes the exception, the record that failed and a custom error message.

The second step is sending the new stream of `ProcessingResult<V, V2>` to the `TopologyErrorHandler.catchErrors()` method, which will split the
stream into two branches:
- The first branch will contain the `ProcessingError` and will be routed to the DLQ topic as a `KafkaError` Avro objects that contains
multiple useful information such as the topic, the partition, the offsets, the exception, and the custom error message of the failed record.
- The second branch will only contain the successful records and will be returned to continue the processing.

#### Production and Deserialization

The library provides handlers for production and deserialization errors, which can be used to route these errors to the configured DLQ topic.

Here's how to use them:

```yml
kafka:
  properties:
    ...
    default.production.exception.handler: com.michelin.kstreamplify.error.DlqProductionExceptionHandler
    default.deserialization.exception.handler: com.michelin.kstreamplify.error.DlqDeserializationExceptionHandler
    ...
```

#### Avro Schema

An Avro schema needs to be deployed in a Schema Registry on top of the DLQ topic. It is available [here](https://github.com/michelin/kstreamplify/blob/main/kstreamplify-core/src/main/avro/kafka-error.avsc).

### REST endpoints

The Kstreamplify library provides several REST endpoints, which are listed below:

- `GET /ready`: This endpoint is used as a readiness probe for Kubernetes deployment.
- `GET /liveness`: This endpoint is used as a liveness probe for Kubernetes deployment.
- `GET /topology`: This endpoint returns the Kafka Streams topology as JSON.

### Testing

For testing, you can create a test class that implements `KafkaStreamsStarterTest` and override the `topology` method. Then, apply the topology of your Kafka Streams on the given `streamsBuilders`.

Here is an example:

```java
public class MyKafkaStreamsTest extends KafkaStreamsStarterTest {
    @Override
    public void topology(StreamsBuilder streamsBuilder) { 
        new MyKafkaStreams().topology(streamsBuilder);
    }
}
```

## Motivation

Developing applications with Kafka Streams can be challenging and often raises many questions for developers. It involves considerations such as efficient bootstrapping of Kafka Streams applications, handling unexpected business issues, and integrating Kubernetes probes, among others.

To assist developers in overcoming these challenges, we have built this library. Our aim is to provide a comprehensive solution that simplifies the development process and addresses common pain points encountered while working with Kafka Streams.

## Contribution

We welcome contributions from the community! Before you get started, please take a look at our [contribution guide](https://github.com/michelin/kstreamplify/blob/master/CONTRIBUTING.md) to learn about our guidelines and best practices. We appreciate your help in making Kstreamplify a better library for everyone.
