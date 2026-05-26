package company.vk.edu.distrib.compute.tadzhnahal;

import com.google.protobuf.ByteString;
import company.vk.edu.distrib.compute.tadzhnahal.grpc.InternalKvRequest;
import company.vk.edu.distrib.compute.tadzhnahal.grpc.InternalKvResponse;
import company.vk.edu.distrib.compute.tadzhnahal.grpc.InternalKvServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.TimeUnit;

public class TadzhnahalProxyClient {
    private static final String METHOD_GET = "GET";
    private static final String METHOD_PUT = "PUT";
    private static final String METHOD_DELETE = "DELETE";
    private static final String GRPC_PORT_PREFIX = "grpcPort=";
    private static final int REQUEST_TIMEOUT_SECONDS = 2;
    private static final int GRPC_PORT_OFFSET = 1000;

    public ProxyResponse get(String endpoint, String id) throws IOException {
        return forward(endpoint, METHOD_GET, id, null);
    }

    public ProxyResponse put(String endpoint, String id, byte[] body) throws IOException {
        return forward(endpoint, METHOD_PUT, id, body);
    }

    public ProxyResponse delete(String endpoint, String id) throws IOException {
        return forward(endpoint, METHOD_DELETE, id, null);
    }

    private ProxyResponse forward(
            String endpoint,
            String method,
            String id,
            byte[] body
    ) throws IOException {
        GrpcTarget target = parseGrpcTarget(endpoint);
        ManagedChannel channel = ManagedChannelBuilder.forAddress(target.host(), target.port())
                .usePlaintext()
                .build();

        try {
            InternalKvServiceGrpc.InternalKvServiceBlockingStub stub = InternalKvServiceGrpc.newBlockingStub(channel)
                    .withDeadlineAfter(REQUEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);

            InternalKvRequest request = InternalKvRequest.newBuilder()
                    .setMethod(method)
                    .setId(id)
                    .setBody(ByteString.copyFrom(body == null ? new byte[0] : body))
                    .build();

            InternalKvResponse response = stub.handle(request);
            return new ProxyResponse(response.getStatusCode(), response.getBody().toByteArray());
        } catch (StatusRuntimeException e) {
            throw new IOException("Grpc request failed", e);
        } finally {
            channel.shutdownNow();
        }
    }

    private GrpcTarget parseGrpcTarget(String endpoint) {
        URI uri = URI.create(endpoint);
        String host = uri.getHost();

        if (host == null || host.isEmpty()) {
            throw new IllegalArgumentException("Endpoint host is empty: " + endpoint);
        }

        int grpcPort = extractGrpcPort(uri);
        return new GrpcTarget(host, grpcPort);
    }

    private int extractGrpcPort(URI uri) {
        Integer portFromUserInfo = extractGrpcPort(uri.getUserInfo());
        if (portFromUserInfo != null) {
            return portFromUserInfo;
        }

        Integer portFromQuery = extractGrpcPort(uri.getQuery());
        if (portFromQuery != null) {
            return portFromQuery;
        }

        int httpPort = uri.getPort();
        if (httpPort < 0) {
            throw new IllegalArgumentException("Endpoint port is empty: " + uri);
        }

        return buildGrpcPort(httpPort);
    }

    private Integer extractGrpcPort(String parameters) {
        if (parameters == null || parameters.isEmpty()) {
            return null;
        }

        String[] parts = parameters.split("&");
        for (String part : parts) {
            if (part.startsWith(GRPC_PORT_PREFIX)) {
                return parseGrpcPort(part.substring(GRPC_PORT_PREFIX.length()));
            }
        }

        return null;
    }

    private Integer parseGrpcPort(String value) {
        try {
            int port = Integer.parseInt(value);
            if (port <= 0 || port >= 65536) {
                throw new IllegalArgumentException("Grpc port is out of range: " + value);
            }

            return port;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid grpc port: " + value, e);
        }
    }

    private int buildGrpcPort(int httpPort) {
        if (httpPort < 64536) {
            return httpPort + GRPC_PORT_OFFSET;
        }

        return httpPort - GRPC_PORT_OFFSET;
    }

    private record GrpcTarget(String host, int port) {
    }

    public record ProxyResponse(int statusCode, byte[] body) {
    }
}
