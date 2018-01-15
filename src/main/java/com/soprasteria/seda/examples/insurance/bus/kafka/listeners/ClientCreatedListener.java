package com.soprasteria.seda.examples.insurance.bus.kafka.listeners;

import com.soprasteria.seda.examples.insurance.events.ClientCreated;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class ClientCreatedListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClientCreatedListener.class);

    @KafkaListener(topics = "${connector.topic}")
    public void clientCreated(ClientCreated event) {
        LOGGER.info("Client Created event: received - {}", event);
    }
}
