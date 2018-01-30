# Microservice Clients

This microservice implements all logic operations of clients


# Running Microservice

```
mvn package
java -jar target/ch-create-client-0.1.0.jar
```

or

```
mvn spring-boot:run
```

## The run.sh Script

This script is used to wrap how to start/stop the microservice. Write the way you want to start/stop the microservice

# Docker Generation

```
mvn install dockerfile:build
```

# Run the service

This command starts the service with domain-clients name

```
docker run --rm -dit --name domain-clients soprasteria/domain-clients
```

Watching logs

```
docker logs domain-clients -f
```

Stopping the service

```
docker stop domain-clients
```

# Issues

- java.lang.NoSuchMethodError: org.springframework.util.Assert.state(ZLjava/util/function/Supplier;)V

Solved: Update to 2.0.0.M7 of spring-boot and 2.1.0.RC1 of spring-kafka adaptor.

- If spring boot starts and kafka is not up
    - 1. There is no error.
    - 2. If after that kafka starts CreateService never gets recovered. Restart service is needed.

- When tests runs may appear this errors:

[kafka-network-thread-0-ListenerName(PLAINTEXT)-PLAINTEXT-1] DEBUG org.apache.kafka.common.network.Selector - [SocketServer brokerId=0] Connection with /127.0.0.1 disconnected
                                          java.io.EOFException: null
Solved: There is no problem: https://github.com/confluentinc/examples/issues/116

- Arbitrary errors (race condition): If kafka group is the same in tests and in configuration:

Solved: On ClientsBootTests set : testClients as the group of kafkaEmbbedd

```
Map<String, Object> consumerProperties = KafkaTestUtils.consumerProps("testClients", "false", embeddedKafka);
```