package com.soprasteria.seda.examples.insurance.persistence;

import com.soprasteria.seda.examples.insurance.model.Client;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Service
public class ClientRepositoryInMemory implements ClientRepository {

    private Map<UUID, Client> store = new HashMap<>();

    @Override
    public Client findById(UUID id) {
        return store.get(id);
    }

    @Override
    public Client create(Client client) {
        return store.put(client.getId(), client);
    }

    @Override
    public Client update(Client client) {
        return store.replace(client.getId(), client);
    }

    @Override
    public Client delete(Client client) {
        return store.remove(client.getId());
    }
}
