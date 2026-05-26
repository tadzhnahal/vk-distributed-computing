package company.vk.edu.distrib.compute.tadzhnahal;

import company.vk.edu.distrib.compute.KVCluster;
import company.vk.edu.distrib.compute.KVService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class TadzhnahalKVCluster implements KVCluster {
    private static final String LOCALHOST = "localhost";
    private static final String HTTP_PREFIX = "http://";
    private static final String GRPC_PORT_PREFIX = "grpcPort=";
    private static final int GRPC_PORT_OFFSET = 1000;

    private final TadzhnahalKVServiceFactory factory;
    private final List<String> endpoints;
    private final Map<String, Integer> portsByEndpoint;
    private final Map<String, Integer> grpcPortsByEndpoint;
    private final Map<String, KVService> startedNodes;

    public TadzhnahalKVCluster(List<Integer> ports) {
        this(ports, TadzhnahalShardingAlgorithm.RENDEZVOUS);
    }

    public TadzhnahalKVCluster(
            List<Integer> ports,
            TadzhnahalShardingAlgorithm shardingAlgorithm
    ) {
        if (ports == null || ports.isEmpty()) {
            throw new IllegalArgumentException("Ports must not be empty");
        }

        this.factory = new TadzhnahalKVServiceFactory(shardingAlgorithm);
        this.endpoints = new ArrayList<>();
        this.portsByEndpoint = new LinkedHashMap<>();
        this.grpcPortsByEndpoint = new LinkedHashMap<>();
        this.startedNodes = new LinkedHashMap<>();

        for (Integer port : ports) {
            if (port == null) {
                throw new IllegalArgumentException("Port must not be null");
            }

            int grpcPort = buildGrpcPort(port);
            String endpoint = buildEndpoint(port, grpcPort);

            if (portsByEndpoint.containsKey(endpoint)) {
                throw new IllegalArgumentException("Duplicate endpoint: " + endpoint);
            }

            endpoints.add(endpoint);
            portsByEndpoint.put(endpoint, port);
            grpcPortsByEndpoint.put(endpoint, grpcPort);
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
        if (startedNodes.containsKey(endpoint)) {
            return;
        }

        Integer port = portsByEndpoint.get(endpoint);
        Integer grpcPort = grpcPortsByEndpoint.get(endpoint);

        if (port == null || grpcPort == null) {
            throw new IllegalArgumentException("Unknown endpoint: " + endpoint);
        }

        try {
            KVService service = factory.create(port, grpcPort, endpoints);
            service.start();
            startedNodes.put(endpoint, service);
        } catch (IOException e) {
            throw new IllegalStateException("Cannot create node for endpoint " + endpoint, e);
        }
    }

    @Override
    public void stop() {
        List<String> activeEndpoints = new ArrayList<>(startedNodes.keySet());
        for (String endpoint : activeEndpoints) {
            stop(endpoint);
        }
    }

    @Override
    public void stop(String endpoint) {
        KVService service = startedNodes.remove(endpoint);
        if (service == null) {
            return;
        }

        service.stop();
    }

    @Override
    public List<String> getEndpoints() {
        return new ArrayList<>(endpoints);
    }

    private static String buildEndpoint(int port, int grpcPort) {
        return HTTP_PREFIX + GRPC_PORT_PREFIX + grpcPort + "@" + LOCALHOST + ":" + port;
    }

    private static int buildGrpcPort(int port) {
        if (port < 64536) {
            return port + GRPC_PORT_OFFSET;
        }

        return port - GRPC_PORT_OFFSET;
    }
}
