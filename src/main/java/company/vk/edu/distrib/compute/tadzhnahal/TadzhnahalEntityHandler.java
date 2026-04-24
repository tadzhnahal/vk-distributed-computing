package company.vk.edu.distrib.compute.tadzhnahal;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;
import java.io.OutputStream;
import java.util.NoSuchElementException;

public class TadzhnahalEntityHandler implements HttpHandler {
    private static final String ENTITY_PATH = "/v0/entity";

    private static final String METHOD_GET = "GET";
    private static final String METHOD_PUT = "PUT";
    private static final String METHOD_DELETE = "DELETE";

    private static final String INTERNAL_REQUEST_HEADER = "X-Internal-Request";
    private static final String INTERNAL_REQUEST_VALUE = "true";

    private final String localEndpoint;
    private final Dao<byte[]> dao;
    private final TadzhnahalRendezvousHashing rendezvousHashing;
    private final TadzhnahalProxyClient proxyClient;

    public TadzhnahalEntityHandler(
            String localEndpoint,
            Dao<byte[]> dao,
            TadzhnahalRendezvousHashing rendezvousHashing,
            TadzhnahalProxyClient proxyClient
    ) {
        this.localEndpoint = localEndpoint;
        this.dao = dao;
        this.rendezvousHashing = rendezvousHashing;
        this.proxyClient = proxyClient;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        try (exchange) {
            try {
                if (!ENTITY_PATH.equals(exchange.getRequestURI().getPath())) {
                    sendResponse(exchange, 404, new byte[0]);
                    return;
                }

                String id = extractId(exchange);

                if (isInternalRequest(exchange)) {
                    handleLocal(exchange, id);
                    return;
                }

                String targetEndpoint = rendezvousHashing.select(id);

                if (localEndpoint.equals(targetEndpoint)) {
                    handleLocal(exchange, id);
                    return;
                }

                handleProxy(exchange, targetEndpoint, id);
            } catch (IllegalArgumentException e) {
                sendResponse(exchange, 400, new byte[0]);
            } catch (NoSuchElementException e) {
                sendResponse(exchange, 404, new byte[0]);
            }
        }
    }

    private void handleLocal(HttpExchange exchange, String id) throws IOException {
        String method = exchange.getRequestMethod();

        if (METHOD_GET.equals(method)) {
            byte[] value = dao.get(id);
            if (value == null) {
                throw new NoSuchElementException("Value not found");
            }

            sendResponse(exchange, 200, value);
            return;
        }

        if (METHOD_PUT.equals(method)) {
            byte[] value = exchange.getRequestBody().readAllBytes();
            dao.upsert(id, value);
            sendResponse(exchange, 201, new byte[0]);
            return;
        }

        if (METHOD_DELETE.equals(method)) {
            dao.delete(id);
            sendResponse(exchange, 202, new byte[0]);
            return;
        }

        sendResponse(exchange, 405, new byte[0]);
    }

    private void handleProxy(
            HttpExchange exchange,
            String targetEndpoint,
            String id
    ) throws IOException {
        String method = exchange.getRequestMethod();
        TadzhnahalProxyClient.ProxyResponse response;

        if (METHOD_GET.equals(method)) {
            response = proxyClient.get(targetEndpoint, id);
            sendResponse(exchange, response.statusCode(), response.body());
            return;
        }

        if (METHOD_PUT.equals(method)) {
            byte[] value = exchange.getRequestBody().readAllBytes();
            response = proxyClient.put(targetEndpoint, id, value);
            sendResponse(exchange, response.statusCode(), response.body());
            return;
        }

        if (METHOD_DELETE.equals(method)) {
            response = proxyClient.delete(targetEndpoint, id);
            sendResponse(exchange, response.statusCode(), response.body());
            return;
        }

        sendResponse(exchange, 405, new byte[0]);
    }

    private boolean isInternalRequest(HttpExchange exchange) {
        String headerValue = exchange.getRequestHeaders().getFirst(INTERNAL_REQUEST_HEADER);
        return INTERNAL_REQUEST_VALUE.equalsIgnoreCase(headerValue);
    }

    private String extractId(HttpExchange exchange) {
        String query = exchange.getRequestURI().getQuery();

        if (query == null) {
            throw new IllegalArgumentException("Missing query");
        }

        if (!query.startsWith("id=")) {
            throw new IllegalArgumentException("Missing id parameter");
        }

        if (query.contains("&")) {
            throw new IllegalArgumentException("Unexpected parameters");
        }

        String id = query.substring("id=".length());

        if (id.isEmpty()) {
            throw new IllegalArgumentException("Empty id");
        }

        return id;
    }

    private void sendResponse(HttpExchange exchange, int code, byte[] body) throws IOException {
        byte[] responseBody = body == null ? new byte[0] : body;

        if (responseBody.length == 0 && code >= 400) {
            responseBody = new byte[] {0};
        }

        if (responseBody.length == 0) {
            exchange.sendResponseHeaders(code, -1);
            return;
        }

        exchange.sendResponseHeaders(code, responseBody.length);
        try (OutputStream outputStream = exchange.getResponseBody()) {
            outputStream.write(responseBody);
        }
    }
}
