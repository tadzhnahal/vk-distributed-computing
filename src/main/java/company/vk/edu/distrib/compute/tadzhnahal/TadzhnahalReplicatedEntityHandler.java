package company.vk.edu.distrib.compute.tadzhnahal;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import company.vk.edu.distrib.compute.tadzhnahal.TadzhnahalReplicaRequestParser.TadzhnahalReplicaRequest;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

public class TadzhnahalReplicatedEntityHandler implements HttpHandler {
    private static final String ENTITY_PATH = "/v0/entity";
    private static final String METHOD_GET = "GET";
    private static final String METHOD_PUT = "PUT";
    private static final String METHOD_DELETE = "DELETE";

    private final TadzhnahalReplicaManager replicaManager;
    private final TadzhnahalReplicaRequestParser requestParser;

    public TadzhnahalReplicatedEntityHandler(TadzhnahalReplicaManager replicaManager) {
        this.replicaManager = replicaManager;
        this.requestParser = new TadzhnahalReplicaRequestParser(replicaManager.replicaCount());
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        try (exchange) {
            if (!ENTITY_PATH.equals(exchange.getRequestURI().getPath())) {
                sendResponse(exchange, 404, new byte[] {0});
                return;
            }

            TadzhnahalReplicaRequest request;
            try {
                request = requestParser.parse(exchange);
            } catch (RuntimeException e) {
                sendResponse(exchange, 400, new byte[] {0});
                return;
            }

            try {
                handleByMethod(exchange, request);
            } catch (RuntimeException e) {
                sendResponse(exchange, 500, new byte[] {0});
            }
        }
    }

    private void handleByMethod(
            HttpExchange exchange,
            TadzhnahalReplicaRequest request
    ) throws IOException {
        String method = exchange.getRequestMethod();

        if (METHOD_GET.equals(method)) {
            handleGet(exchange, request);
            return;
        }

        if (METHOD_PUT.equals(method)) {
            handlePut(exchange, request);
            return;
        }

        if (METHOD_DELETE.equals(method)) {
            handleDelete(exchange, request);
            return;
        }

        sendResponse(exchange, 405, new byte[] {0});
    }

    private void handleGet(
            HttpExchange exchange,
            TadzhnahalReplicaRequest request
    ) throws IOException {
        List<Integer> replicaIds = replicaManager.replicaIdsForKey(request.id());
        List<TadzhnahalReplicaRecord> records = new ArrayList<>();
        int successCount = 0;

        for (Integer replicaId : replicaIds) {
            TadzhnahalReplicaRecord record = tryReadRecord(replicaId, request.id());

            if (record != null) {
                records.add(record);
            }

            if (replicaManager.isReplicaEnabled(replicaId)) {
                successCount++;
            }
        }

        if (successCount < request.ack()) {
            sendResponse(exchange, 500, new byte[] {0});
            return;
        }

        if (records.isEmpty()) {
            sendResponse(exchange, 404, new byte[] {0});
            return;
        }

        TadzhnahalReplicaRecord latestRecord = replicaManager.latestRecord(records);
        if (latestRecord == null || latestRecord.deleted()) {
            sendResponse(exchange, 404, new byte[] {0});
            return;
        }

        sendResponse(exchange, 200, latestRecord.value());
    }

    private void handlePut(
            HttpExchange exchange,
            TadzhnahalReplicaRequest request
    ) throws IOException {
        byte[] value = exchange.getRequestBody().readAllBytes();
        long version = replicaManager.nextVersion();
        TadzhnahalReplicaRecord record = new TadzhnahalReplicaRecord(value, version, false);

        int successCount = replicateWrite(request.id(), record);
        if (successCount < request.ack()) {
            sendResponse(exchange, 500, new byte[] {0});
            return;
        }

        sendResponse(exchange, 201, new byte[0]);
    }

    private void handleDelete(
            HttpExchange exchange,
            TadzhnahalReplicaRequest request
    ) throws IOException {
        long version = replicaManager.nextVersion();
        int successCount = replicateDelete(request.id(), version);

        if (successCount < request.ack()) {
            sendResponse(exchange, 500, new byte[] {0});
            return;
        }

        sendResponse(exchange, 202, new byte[0]);
    }

    private int replicateWrite(String key, TadzhnahalReplicaRecord record) {
        int successCount = 0;

        for (Integer replicaId : replicaManager.replicaIdsForKey(key)) {
            try {
                replicaManager.writeRecord(replicaId, key, record);
                successCount++;
            } catch (IOException ignored) {
                // реплика недоступна
            }
        }

        return successCount;
    }

    private int replicateDelete(String key, long version) {
        int successCount = 0;

        for (Integer replicaId : replicaManager.replicaIdsForKey(key)) {
            try {
                replicaManager.writeTombstone(replicaId, key, version);
                successCount++;
            } catch (IOException ignored) {
                // реплика недоступна
            }
        }

        return successCount;
    }

    private TadzhnahalReplicaRecord tryReadRecord(int replicaId, String key) {
        try {
            return replicaManager.readRecord(replicaId, key);
        } catch (IOException | RuntimeException e) {
            return null;
        }
    }

    private void sendResponse(HttpExchange exchange, int code, byte[] body) throws IOException {
        byte[] responseBody = body == null ? new byte[0] : body;

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
