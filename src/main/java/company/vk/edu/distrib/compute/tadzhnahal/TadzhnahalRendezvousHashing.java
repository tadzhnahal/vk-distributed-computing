package company.vk.edu.distrib.compute.tadzhnahal;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public class TadzhnahalRendezvousHashing {
    private final List<String> endpoints;

    public TadzhnahalRendezvousHashing(List<String> endpoints) {
        if (endpoints == null || endpoints.isEmpty()) {
            throw new IllegalArgumentException("Endpoints must not be empty");
        }

        Set<String> uniqueEndpoints = new LinkedHashSet<>();
        for (String endpoint : endpoints) {
            if (endpoint == null || endpoint.isEmpty()) {
                throw new IllegalArgumentException("Endpoint must not be empty");
            }
        }

        this.endpoints = List.copyOf(uniqueEndpoints);
    }

    public String select(String key) {
        if (key == null) {
            throw new IllegalArgumentException("Key must not be null");
        }

        String bestEndpoint = null;
        long bestScore = 0L;

        for (String endpoint : endpoints) {
            long currentScore = score(endpoint, key);
            if (bestEndpoint == null || Long.compareUnsigned(currentScore, bestScore) > 0) {
                bestEndpoint = endpoint;
                bestScore = currentScore;
            }
        }

        return bestEndpoint;
    }

    private long score(String endpoint, String key) {
        long hash = 0xcbf29ce484222325L;
        String value = endpoint + '\n' + key;

        for (int i = 0; i < value.length(); i++) {
            hash ^= value.charAt(i);
            hash *= 0x100000001b3L;
        }

        return hash;
    }
}
