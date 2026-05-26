package company.vk.edu.distrib.compute.tadzhnahal;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.ReplicatedService;
import io.grpc.Server;
import io.grpc.ServerBuilder;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class TadzhnahalKVService implements ReplicatedService {
    private static final String STATUS_PATH = "/v0/status";
    private static final String ENTITY_PATH = "/v0/entity";
    private static final String METHOD_GET = "GET";
    private static final String LOCALHOST = "http://localhost:";
    private static final String GRPC_PORT_PARAM = "?grpcPort=";
    private static final int GRPC_PORT_OFFSET = 1000;

    private final int port;
    private final int grpcPort;
    private final Path rootDir;
    private final TadzhnahalReplicaManager replicaManager;

    private final String localEndpoint;
    private final List<String> clusterEndpoints;
    private final TadzhnahalRendezvousHashing rendezvousHashing;
    private final TadzhnahalProxyClient proxyClient;

    private HttpServer server;
    private Server grpcServer;
    private boolean started;

    public TadzhnahalKVService(int port, Path rootDir, int replicaCount) throws IOException {
        this(
                port,
                rootDir,
                replicaCount,
                buildGrpcPort(port),
                List.of(buildEndpoint(port, buildGrpcPort(port)))
        );
    }

    public TadzhnahalKVService(
            int port,
            Path rootDir,
            int replicaCount,
            List<String> clusterEndpoints
    ) throws IOException {
        this(port, rootDir, replicaCount, buildGrpcPort(port), clusterEndpoints);
    }

    public TadzhnahalKVService(
            int port,
            Path rootDir,
            int replicaCount,
            int grpcPort,
            List<String> clusterEndpoints
    ) throws IOException {
        if (rootDir == null) {
            throw new IllegalArgumentException("Root dir must not be null");
        }

        if (replicaCount < 1) {
            throw new IllegalArgumentException("Replica count must be positive");
        }

        if (grpcPort <= 0 || grpcPort >= 65536) {
            throw new IllegalArgumentException("Grpc port is out of range");
        }

        if (clusterEndpoints == null || clusterEndpoints.isEmpty()) {
            throw new IllegalArgumentException("Cluster endpoints must not be empty");
        }

        this.port = port;
        this.grpcPort = grpcPort;
        this.rootDir = rootDir;
        this.replicaManager = new TadzhnahalReplicaManager(rootDir, replicaCount);

        this.localEndpoint = buildEndpoint(port, grpcPort);
        this.clusterEndpoints = prepareClusterEndpoints(clusterEndpoints, localEndpoint);
        this.rendezvousHashing = new TadzhnahalRendezvousHashing(this.clusterEndpoints);
        this.proxyClient = new TadzhnahalProxyClient();
    }

    @Override
    public void start() {
        if (started) {
            throw new IllegalStateException("Server already started");
        }

        try {
            startGrpcServer();
            startHttpServer();
            started = true;
        } catch (IOException e) {
            stopGrpcServer();
            throw new IllegalStateException("Cannot start server", e);
        }
    }

    @Override
    public void stop() {
        if (!started) {
            throw new IllegalStateException("Server is not started");
        }

        if (server != null) {
            server.stop(0);
            server = null;
        }

        stopGrpcServer();
        started = false;
    }

    @Override
    public int port() {
        return port;
    }

    public int grpcPort() {
        return grpcPort;
    }

    @Override
    public int numberOfReplicas() {
        return replicaManager.replicaCount();
    }

    @Override
    public void disableReplica(int nodeId) {
        replicaManager.disableReplica(nodeId);
    }

    @Override
    public void enableReplica(int nodeId) {
        replicaManager.enableReplica(nodeId);
    }

    public Path rootDir() {
        return rootDir;
    }

    TadzhnahalReplicaManager replicaManager() {
        return replicaManager;
    }

    private void startGrpcServer() throws IOException {
        grpcServer = ServerBuilder.forPort(grpcPort)
                .addService(new TadzhnahalInternalKvService(replicaManager.replicaNodes().get(0).dao()))
                .build()
                .start();
    }

    private void stopGrpcServer() {
        if (grpcServer == null) {
            return;
        }

        grpcServer.shutdownNow();
        grpcServer = null;
    }

    private void startHttpServer() throws IOException {
        server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext(STATUS_PATH, this::handleStatus);

        if (clusterEndpoints.size() == 1) {
            server.createContext(
                    ENTITY_PATH,
                    new TadzhnahalReplicatedEntityHandler(replicaManager)
            );
        } else {
            server.createContext(
                    ENTITY_PATH,
                    new TadzhnahalEntityHandler(
                            localEndpoint,
                            replicaManager.replicaNodes().get(0).dao(),
                            rendezvousHashing,
                            proxyClient
                    )
            );
        }

        server.start();
    }

    private void handleStatus(HttpExchange exchange) throws IOException {
        try (exchange) {
            if (!STATUS_PATH.equals(exchange.getRequestURI().getPath())) {
                sendEmptyResponse(exchange, 404);
                return;
            }

            if (!METHOD_GET.equals(exchange.getRequestMethod())) {
                sendEmptyResponse(exchange, 405);
                return;
            }

            sendEmptyResponse(exchange, 200);
        }
    }

    private void sendEmptyResponse(HttpExchange exchange, int code) throws IOException {
        exchange.sendResponseHeaders(code, -1);
    }

    private static String buildEndpoint(int port, int grpcPort) {
        return LOCALHOST + port + GRPC_PORT_PARAM + grpcPort;
    }

    private static int buildGrpcPort(int port) {
        if (port < 64536) {
            return port + GRPC_PORT_OFFSET;
        }

        return port - GRPC_PORT_OFFSET;
    }

    private static List<String> prepareClusterEndpoints(
            List<String> clusterEndpoints,
            String localEndpoint
    ) {
        List<String> endpoints = new ArrayList<>(clusterEndpoints);

        if (!endpoints.contains(localEndpoint)) {
            endpoints.add(localEndpoint);
        }

        return List.copyOf(endpoints);
    }
}
