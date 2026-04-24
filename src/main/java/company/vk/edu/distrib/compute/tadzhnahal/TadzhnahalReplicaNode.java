package company.vk.edu.distrib.compute.tadzhnahal;

import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;

public class TadzhnahalReplicaNode {
    private final int nodeId;
    private final Dao<byte[]> dao;
    private boolean enabled;

    public TadzhnahalReplicaNode(int nodeId, Dao<byte[]> dao) {
        if (dao == null) {
            throw new IllegalArgumentException("Dao must not be null");
        }

        this.nodeId = nodeId;
        this.dao = dao;
        this.enabled = true;
    }

    public int nodeId() {
        return nodeId;
    }

    public boolean enabled() {
        return enabled;
    }

    public void enable() {
        enabled = true;
    }

    public void disable() {
        enabled = false;
    }

    public byte[] getRaw(String key) throws IOException {
        return dao.get(key);
    }

    public void upsertRaw(String key, byte[] value) throws IOException {
        dao.upsert(key, value);
    }

    public void deleteRaw(String key) throws IOException {
        dao.delete(key);
    }

    public void close() throws IOException {
        dao.close();
    }
}
