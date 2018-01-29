package com.soprasteria.seda.examples.insurance;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.soprasteria.seda.examples.insurance.bus.producer.Sender;
import com.soprasteria.seda.examples.insurance.events.AbstractEvent;
import com.soprasteria.seda.examples.insurance.events.ClientCreated;
import com.soprasteria.seda.examples.insurance.events.ClientStored;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
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

@RunWith(SpringRunner.class)
@SpringBootTest
@DirtiesContext
@EmbeddedKafka(partitions = 1, topics = { ClientsBootTests.domain })
public class ClientsBootTests {

    private static final String app = "createClient";
    protected static final String domain = "production";

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    private ObjectMapper mapper = new ObjectMapper();

	private KafkaMessageListenerContainer<String, Object> container;

	private BlockingQueue<ConsumerRecord<String, Object>> records;

	@Autowired
	private KafkaEmbedded embeddedKafka;

	@Before
	public void setUp() throws Exception {
		// set up the Kafka consumer properties
		Map<String, Object> consumerProperties = KafkaTestUtils.consumerProps("testClients", "false", embeddedKafka);
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);

		// create a Kafka consumer factory
        JsonDeserializer des = new JsonDeserializer<>(Object.class);
        des.addTrustedPackages("com.soprasteria.seda.examples.insurance.events");
		DefaultKafkaConsumerFactory<String, Object> consumerFactory = new DefaultKafkaConsumerFactory<String, Object>(consumerProperties, new StringDeserializer(), des);

		// set the topic that needs to be consumed
		ContainerProperties containerProperties = new ContainerProperties(domain);

		// create a Kafka MessageListenerContainer
		container = new KafkaMessageListenerContainer<>(consumerFactory, containerProperties);

		// create a thread safe queue to store the received message
		records = new LinkedBlockingQueue<>();

		// setup a Kafka message listener
		container.setupMessageListener(new MessageListener<String, Object>() {
			@Override
			public void onMessage(ConsumerRecord<String, Object> record) {
			    if (record.topic().equals(domain))
				    records.add(record);
			}
		});

		// start the container and underlying message listener
		container.start();

		// wait until the container has the required number of assigned partitions
		ContainerTestUtils.waitForAssignment(container, embeddedKafka.getPartitionsPerTopic());
	}

	@After
	public void tearDown() {
		// stop the container
		container.stop();
	}

	@Test
    public void clientCreateSendClientStored() throws Exception {

        ClientCreated created = mapper.readValue("{\"type\": \"ClientCreated\", \"name\": \"John Doe\", \"address\": \"Bendford st 10\", \"interest\": \"Microservices\", \"id\":\"82565d32-45ea-40c5-9f56-9d2a93a648a0\"}", ClientCreated.class);
        String exceptedType = "ClientStored";

        // Send mocked events --------------------

        kafkaTemplate.send(app, created);

        // Check results events

		ConsumerRecord<String, Object> rec = records.poll(10, TimeUnit.SECONDS);

		assertThat(rec).isNotNull();
        ClientStored stored = (ClientStored) rec.value();

        assertThat(stored).isNotNull();
        assertThat(stored.getType()).isEqualTo(exceptedType);
        assertThat(stored.getId()).isEqualByComparingTo(created.getId());
        assertThat(stored.getName()).isEqualTo(created.getName());
	}
}