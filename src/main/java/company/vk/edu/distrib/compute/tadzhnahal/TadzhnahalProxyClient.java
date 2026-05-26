package company.vk.edu.distrib.compute.tadzhnahal;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

public class TadzhnahalProxyClient {
    private static final String ENTITY_PATH = "/v0/entity?id=";
    private static final String INTERNAL_REQUEST_HEADER = "X-Internal-Request";
    private static final String INTERNAL_REQUEST_VALUE = "true";
    private static final char QUERY_SEPARATOR = '?';

    private final HttpClient httpClient;

    public TadzhnahalProxyClient() {
        this.httpClient = HttpClient.newHttpClient();
    }

    public ProxyResponse get(String endpoint, String id) throws IOException {
        HttpResponse<byte[]> response = forward(endpoint, "GET", id, null);
        return toProxyResponse(response);
    }

    public ProxyResponse put(String endpoint, String id, byte[] body) throws IOException {
        HttpResponse<byte[]> response = forward(endpoint, "PUT", id, body);
        return toProxyResponse(response);
    }

    public ProxyResponse delete(String endpoint, String id) throws IOException {
        HttpResponse<byte[]> response = forward(endpoint, "DELETE", id, null);
        return toProxyResponse(response);
    }

    public HttpResponse<byte[]> forward(
            String endpoint,
            String method,
            String id,
            byte[] body
    ) throws IOException {
        HttpRequest.Builder builder = requestBuilder(endpoint, id);
        byte[] requestBody = body == null ? new byte[0] : body;

        if ("GET".equals(method)) {
            builder.GET();
        } else if ("PUT".equals(method)) {
            builder.PUT(HttpRequest.BodyPublishers.ofByteArray(requestBody));
        } else if ("DELETE".equals(method)) {
            builder.DELETE();
        } else {
            throw new IllegalArgumentException("Unsupported method: " + method);
        }

        try {
            return httpClient.send(
                    builder.build(),
                    HttpResponse.BodyHandlers.ofByteArray()
            );
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Request interrupted", e);
        }
    }

    private HttpRequest.Builder requestBuilder(String endpoint, String id) {
        String encodedId = URLEncoder.encode(id, StandardCharsets.UTF_8);
        URI uri = URI.create(httpEndpoint(endpoint) + ENTITY_PATH + encodedId);

        return HttpRequest.newBuilder(uri)
                .timeout(Duration.ofSeconds(2))
                .header(INTERNAL_REQUEST_HEADER, INTERNAL_REQUEST_VALUE);
    }

    private String httpEndpoint(String endpoint) {
        int queryIndex = endpoint.indexOf(QUERY_SEPARATOR);
        if (queryIndex < 0) {
            return endpoint;
        }

        return endpoint.substring(0, queryIndex);
    }

    private ProxyResponse toProxyResponse(HttpResponse<byte[]> response) {
        byte[] body = response.body();
        if (body == null) {
            body = new byte[0];
        }

        return new ProxyResponse(response.statusCode(), body);
    }

    public record ProxyResponse(int statusCode, byte[] body) {
    }
}
