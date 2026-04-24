package company.vk.edu.distrib.compute.tadzhnahal;

import com.sun.net.httpserver.HttpExchange;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;

public final class TadzhnahalReplicaRequestParser {
    private static final String ID_PARAM = "id";
    private static final String ACK_PARAM = "ack";

    private final int replicaCount;

    public TadzhnahalReplicaRequestParser(int replicaCount) {
        this.replicaCount = replicaCount;
    }

    public TadzhnahalReplicaRequest parse(HttpExchange exchange) {
        String query = exchange.getRequestURI().getRawQuery();
        String id = extractRequiredParameter(query, ID_PARAM);
        validateId(id);

        Integer ack = extractOptionalIntParameter(query, ACK_PARAM);
        int effectiveAck = ack == null ? 1 : ack;
        validateAck(effectiveAck);

        return new TadzhnahalReplicaRequest(id, effectiveAck);
    }

    private String extractRequiredParameter(String query, String parameterName) {
        String value = extractParameter(query, parameterName);
        if (value == null) {
            throw new IllegalArgumentException("missing parameter: " + parameterName);
        }
        return value;
    }

    private Integer extractOptionalIntParameter(String query, String parameterName) {
        String value = extractParameter(query, parameterName);
        if (value == null) {
            return null;
        }

        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("invalid integer parameter: " + parameterName, e);
        }
    }

    private String extractParameter(String query, String parameterName) {
        if (query == null || query.isBlank()) {
            return null;
        }

        String[] parts = query.split("&");
        for (String part : parts) {
            int separatorIndex = part.indexOf('=');
            if (separatorIndex < 0) {
                String key = decode(part);
                if (parameterName.equals(key)) {
                    return "";
                }
                continue;
            }

            String key = decode(part.substring(0, separatorIndex));
            if (!parameterName.equals(key)) {
                continue;
            }

            String rawValue = part.substring(separatorIndex + 1);
            return decode(rawValue);
        }

        return null;
    }

    private void validateId(String id) {
        if (id.isEmpty()) {
            throw new IllegalArgumentException("id must not be empty");
        }
    }

    private void validateAck(int ack) {
        if (ack < 1 || ack > replicaCount) {
            throw new IllegalArgumentException("ack is out of range");
        }
    }

    private String decode(String value) {
        return URLDecoder.decode(value, StandardCharsets.UTF_8);
    }

    public record TadzhnahalReplicaRequest(String id, int ack) {
    }
}
