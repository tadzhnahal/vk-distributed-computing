package company.vk.edu.distrib.compute.tadzhnahal;

import com.google.protobuf.ByteString;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.tadzhnahal.grpc.InternalKvRequest;
import company.vk.edu.distrib.compute.tadzhnahal.grpc.InternalKvResponse;
import company.vk.edu.distrib.compute.tadzhnahal.grpc.InternalKvServiceGrpc;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.util.NoSuchElementException;

public class TadzhnahalInternalKvService extends InternalKvServiceGrpc.InternalKvServiceImplBase {
    private static final String METHOD_GET = "GET";
    private static final String METHOD_PUT = "PUT";
    private static final String METHOD_DELETE = "DELETE";

    private final Dao<byte[]> dao;

    public TadzhnahalInternalKvService(Dao<byte[]> dao) {
        super();

        if (dao == null) {
            throw new IllegalArgumentException("Dao must not be null");
        }

        this.dao = dao;
    }

    @Override
    public void handle(
            InternalKvRequest request,
            StreamObserver<InternalKvResponse> responseObserver
    ) {
        InternalKvResponse response = handleRequest(request);
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    private InternalKvResponse handleRequest(InternalKvRequest request) {
        try {
            String method = request.getMethod();
            String id = request.getId();

            if (id == null || id.isEmpty()) {
                return response(400, new byte[0]);
            }

            if (METHOD_GET.equals(method)) {
                return handleGet(id);
            }

            if (METHOD_PUT.equals(method)) {
                return handlePut(id, request.getBody().toByteArray());
            }

            if (METHOD_DELETE.equals(method)) {
                return handleDelete(id);
            }

            return response(405, new byte[0]);
        } catch (RuntimeException e) {
            return response(500, new byte [0]);
        }
    }

    private InternalKvResponse handleGet(String id) {
        try {
            byte[] value = dao.get(id);
            return response(200, value);
        } catch (NoSuchElementException e) {
            return response(404, new byte[0]);
        } catch (IOException e) {
            return response(500, new byte[0]);
        }
    }

    private InternalKvResponse handlePut(String id, byte[] body) {
        try {
            dao.upsert(id, body);
            return response(201, new byte[0]);
        } catch (IOException e) {
            return response(500, new byte[0]);
        }
    }

    private InternalKvResponse handleDelete(String id) {
        try {
            dao.delete(id);
            return response(202, new byte[0]);
        } catch (IOException e) {
            return response(500, new byte[0]);
        }
    }

    private InternalKvResponse response(int statusCode, byte[] body) {
        byte[] responseBody = body == null ? new byte[0] : body;

        return InternalKvResponse.newBuilder()
                .setStatusCode(statusCode)
                .setBody(ByteString.copyFrom(responseBody))
                .build();
    }
}
