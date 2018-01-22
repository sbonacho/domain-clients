package com.soprasteria.seda.examples.insurance.bus.kafka.listeners;

import com.soprasteria.seda.examples.insurance.bus.producer.Sender;
import com.soprasteria.seda.examples.insurance.events.*;
import com.soprasteria.seda.examples.insurance.model.Client;
import com.soprasteria.seda.examples.insurance.persistence.ClientRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class ClientsListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClientsListener.class);

    @Autowired
    private Sender<AbstractEvent> sender;

    @Autowired
    private ClientRepository repository;

    @KafkaListener(topics = "${connector.topics.app}")
    public void clientCreated(ClientCreated event) {
        LOGGER.info("ClientCreated Event Received -> {}", event);

        Client client = new Client();
        client.setId(event.getId());
        client.setName(event.getName());
        client = repository.create(client);
        //TODO: What happen if an Exception is sent here

        ClientStored stored = new ClientStored();
        stored.setId(client.getId());
        stored.setName(client.getName());

        LOGGER.info("ClientStored Event Emitted -> {}", stored);
        sender.send(stored);
    }

    @KafkaListener(topics = "${connector.topics.domain}")
    public void clientDeleted(AbstractEvent event) {
        if (event instanceof ClientDeleted) {
            LOGGER.info("ClientDeleted Event Received -> {}", event);
            Client client = repository.findById(event.getId());
            client = repository.delete(client);

            ClientDone done = new ClientDone();
            done.setId(client.getId());

            LOGGER.info("ClientDeleted Done! Event Emitted -> {}", done);
            sender.send(done);
        }
    }
}
