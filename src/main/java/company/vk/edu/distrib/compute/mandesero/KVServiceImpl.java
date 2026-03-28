package company.vk.edu.distrib.compute.mandesero;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVService;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class KVServiceImpl implements KVService {
    private static final Logger log = LoggerFactory.getLogger(KVServiceImpl.class);
    private static final int MIN_PORT = 1;
    private static final int MAX_PORT = 65_535;
    private static final String GET_METHOD = "GET";
    private static final String PUT_METHOD = "PUT";
    private static final String DELETE_METHOD = "DELETE";

    private final Dao<byte[]> dao;

    private final HttpServer server;
    private final InetSocketAddress listenAddress;
    private boolean started;

    public KVServiceImpl(int port, Dao<byte[]> dao) throws IOException {
        this.dao = validateDao(dao);
        this.listenAddress = createListenAddress(port);
        this.server = HttpServer.create();
        createContexts();
    }

    /**
     * Bind storage to HTTP port and start listening.
     *
     * <p>
     * May be called only once.
     */
    @Override
    public void start() {
        if (started) {
            throw new IllegalStateException("Service already started");
        }

        try {
            server.bind(listenAddress, 0);
            if (log.isDebugEnabled()) {
                log.debug("HTTP server bound to {}", server.getAddress());
            }
            server.start();
            if (log.isDebugEnabled()) {
                log.debug("HTTP server started on {}", server.getAddress());
            }
            started = true;
        } catch (IOException e) {
            log.error("Failed to start HTTP server on {}", listenAddress, e);
            throw new UncheckedIOException("Failed to start HTTP server", e);
        }
    }

    /**
     * Stop listening and free all the resources.
     *
     * <p>
     * May be called only once and after {@link #start()}.
     */
    @Override
    public void stop() {
        if (!started) {
            throw new IllegalStateException("Service is not started");
        }

        server.stop(0);
        started = false;
        if (log.isDebugEnabled()) {
            log.debug("HTTP server stopped");
        }
    }

    private void createContexts() {
        server.createContext("/v0/status", wrapHandler(this::handleStatus));
        server.createContext("/v0/entity", wrapHandler(this::handleEntity));
    }

    private void handleStatus(HttpExchange exchange) throws IOException {
        String requestMethod = exchange.getRequestMethod();
        if (!Objects.equals(requestMethod, GET_METHOD)) {
            exchange.sendResponseHeaders(405, -1);
            return;
        }
        exchange.sendResponseHeaders(200, -1);
    }

    private void handleEntity(HttpExchange exchange) throws IOException {
        Map<String, String> params = parseQueryParams(exchange);

        String id = params.get("id");
        if (Objects.isNull(id)) {
            exchange.sendResponseHeaders(400, -1);
            return;
        }

        String requestMethod = exchange.getRequestMethod();
        switch (requestMethod) {
            case GET_METHOD: {
                byte[] value = dao.get(id);
                exchange.sendResponseHeaders(200, value.length);
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(value);
                }
                break;
            }
            case PUT_METHOD: {
                byte[] newValue = exchange.getRequestBody().readAllBytes();
                dao.upsert(id, newValue);
                exchange.sendResponseHeaders(201, -1);
                break;
            }
            case DELETE_METHOD: {
                dao.delete(id);
                exchange.sendResponseHeaders(202, -1);
                break;
            }
            default: {
                exchange.sendResponseHeaders(405, -1);
                break;
            }
        }
    }

    private static Map<String, String> parseQueryParams(HttpExchange exchange) {
        String rawQuery = exchange.getRequestURI().getRawQuery();
        ConcurrentMap<String, String> params = new ConcurrentHashMap<>();

        if (rawQuery == null || rawQuery.isEmpty()) {
            return params;
        }

        for (String pair : rawQuery.split("&")) {
            String[] parts = pair.split("=", 2);

            String key = URLDecoder.decode(parts[0], StandardCharsets.UTF_8);
            String value = parts.length > 1
                    ? URLDecoder.decode(parts[1], StandardCharsets.UTF_8)
                    : "";

            params.put(key, value);
        }

        return params;
    }

    private HttpHandler wrapHandler(HttpHandler handler) {
        return exchange -> {
            try (exchange) {
                try {
                    if (log.isDebugEnabled()) {
                        log.debug("Handling {} {}", exchange.getRequestMethod(), exchange.getRequestURI());
                    }
                    handler.handle(exchange);
                } catch (NoSuchElementException e) {
                    sendError(exchange, 404, e.getMessage());
                } catch (IllegalArgumentException e) {
                    sendError(exchange, 400, e.getMessage());
                } catch (Exception e) {
                    sendError(exchange, 503, e.getMessage());
                }
            }
        };
    }

    private void sendError(HttpExchange exchange, int statusCode, String message) throws IOException {
        String response = message == null ? "" : message;
        byte[] bytes = response.getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(statusCode, bytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }

    private static Dao<byte[]> validateDao(Dao<byte[]> dao) {
        if (dao == null) {
            throw new IllegalArgumentException("dao must not be null");
        }

        return dao;
    }

    @SuppressFBWarnings(
            value = "URLCONNECTION_SSRF_FD",
            justification = "Local server bind only"
    )
    private static InetSocketAddress createListenAddress(int port) {
        validatePort(port);
        return new InetSocketAddress(InetAddress.getLoopbackAddress(), port);
    }

    private static void validatePort(int port) {
        if (port < MIN_PORT || port > MAX_PORT) {
            throw new IllegalArgumentException("port must be in range 1..65535");
        }
    }
}
