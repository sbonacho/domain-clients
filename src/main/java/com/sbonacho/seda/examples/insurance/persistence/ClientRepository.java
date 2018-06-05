package com.sbonacho.seda.examples.insurance.persistence;

import com.sbonacho.seda.examples.insurance.model.Client;

import java.util.UUID;

public interface ClientRepository {
    Client findById(UUID id);
    Client create(Client client);
    Client update(Client client);
    Client delete(Client client);
}
