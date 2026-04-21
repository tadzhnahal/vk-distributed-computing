package company.vk.edu.distrib.compute.tadzhnahal;

import company.vk.edu.distrib.compute.KVCluster;
import company.vk.edu.distrib.compute.KVService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TadzhnahalKVCluster implements KVCluster {
    private static final String LOCALHOST = "http://localhost:";

    private final List<String> endpoints;
    private final Map<String, KVService> nodes;
    private final Set<String> startedEndpoints;

    public TadzhnahalKVCluster(List<Integer> ports) {
        this.endpoints = new ArrayList<>();
        this.nodes = new LinkedHashMap<>();
        this.startedEndpoints = new HashSet<>();

        TadzhnahalKVServiceFactory factory = new TadzhnahalKVServiceFactory();

        for (Integer port : ports) {
            String endpoint = LOCALHOST + port;
            endpoints.add(endpoint);

            try {
                nodes.put(endpoint, factory.create(port));
            } catch (IOException e) {
                throw new IllegalStateException("Cant create node for endpoint " + endpoint, e);
            }
        }
    }

    @Override
    public void start() {
        for (String endpoint : endpoints) {
            start(endpoint);
        }
    }

    @Override
    public void start(String endpoint) {
        KVService service = nodes.get(endpoint);

        if (service == null) {
            throw new IllegalArgumentException("Unknown endpoint: " + endpoint);
        }

        if (startedEndpoints.contains(endpoint)) {
            return;
        }

        service.start();
        startedEndpoints.add(endpoint);
    }

    @Override
    public void stop() {
        List<String> activeEndpoints = new ArrayList<>(startedEndpoints);

        for (String endpoint : activeEndpoints) {
            stop(endpoint);
        }
    }

    @Override
    public void stop(String endpoint) {
        KVService service = nodes.get(endpoint);

        if (service == null) {
            throw new IllegalArgumentException("Unknown endpoint: " + endpoint);
        }

        if (!startedEndpoints.contains(endpoint)) {
            return;
        }

        service.stop();
        startedEndpoints.remove(endpoint);
    }

    @Override
    public List<String> getEndpoints() {
        return new ArrayList<>(endpoints);
    }
}
