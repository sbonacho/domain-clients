package com.soprasteria.seda.examples.insurance;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.soprasteria.seda.examples.insurance.bus.kafka.listeners.ClientsListener;
import com.soprasteria.seda.examples.insurance.events.ClientCreated;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.internals.Sender;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.listener.config.ContainerProperties;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.kafka.test.assertj.KafkaConditions.key;

@RunWith(SpringRunner.class)
@SpringBootTest
@DirtiesContext
@EmbeddedKafka(partitions = 1, topics = { ClientsBootTests.app, ClientsBootTests.domain })
public class ClientsBootTests {
	protected static final String app = "createClient";
	protected static final String domain = "createClient";

	private static final Logger LOGGER = LoggerFactory.getLogger(ClientsListener.class);

	@Autowired
	private Sender sender;

	@Autowired
	private KafkaEmbedded embeddedKafka;

	private KafkaMessageListenerContainer<String, Object> container;

	private BlockingQueue<ConsumerRecord<String, Object>> records;

	private ObjectMapper mapper = new ObjectMapper();

	@Before
	public void setUp() throws Exception {

		Map<String, Object> consumerProperties = KafkaTestUtils.consumerProps("clients", "false", embeddedKafka);
		consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);

		DefaultKafkaConsumerFactory<String, Object> consumerFactory = new DefaultKafkaConsumerFactory<String, Object>(consumerProperties);
		ContainerProperties containerProperties = new ContainerProperties(domain);

		container = new KafkaMessageListenerContainer<>(consumerFactory, containerProperties);
		records = new LinkedBlockingQueue<>();

		container.setupMessageListener(new MessageListener<String, Object>() {
			@Override
			public void onMessage(ConsumerRecord<String, Object> record) {
				LOGGER.debug("test-listener received message='{}'", record.toString());
				records.add(record);
			}
		});

		container.start();
		ContainerTestUtils.waitForAssignment(container, embeddedKafka.getPartitionsPerTopic());
	}

	@After
	public void tearDown() {
		// stop the container
		container.stop();
	}

	@Test
	public void clientCreateSendClientStored() throws Exception {

		String eventName = "ClientStored";

		ClientCreated created = mapper.readValue("{\"type\": \"ClientCreated\", \"name\": \"John Doe\", \"address\": \"Bendford st 10\", \"interest\": \"Microservices\"}", ClientCreated.class);

		ConsumerRecord<String, Object> received = records.poll(10, TimeUnit.SECONDS);
		// Hamcrest Matchers to check the value
		assertThat(received).isNotNull();
		// AssertJ Condition to check the key
		assertThat(received).has(key(null));

	}
}
