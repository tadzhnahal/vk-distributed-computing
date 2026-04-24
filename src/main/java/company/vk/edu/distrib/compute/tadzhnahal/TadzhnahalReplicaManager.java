package company.vk.edu.distrib.compute.tadzhnahal;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

public class TadzhnahalReplicaManager {
    private final int replicaCount;
    private final List<TadzhnahalReplicaNode> replicaNodes;
    private final TadzhnahalReplicaSelector replicaSelector;
    private final TadzhnahalReplicaRecordCodec replicaRecordCodec;
    private final AtomicLong versionCounter;

    public TadzhnahalReplicaManager(Path rootDir, int replicaCount) throws IOException {
        if (rootDir == null) {
            throw new IllegalArgumentException("Root dir must not be null");
        }

        if (replicaCount < 1) {
            throw new IllegalArgumentException("Replica count must be positive");
        }

        this.replicaCount = replicaCount;
        this.replicaNodes = createReplicaNodes(rootDir, replicaCount);
        this.replicaSelector = new TadzhnahalReplicaSelector(replicaCount);
        this.replicaRecordCodec = new TadzhnahalReplicaRecordCodec();
        this.versionCounter = new AtomicLong(System.currentTimeMillis());
    }

    public int replicaCount() {
        return replicaCount;
    }

    public void disableReplica(int nodeId) {
        replica(nodeId).disable();
    }

    public void enableReplica(int nodeId) {
        replica(nodeId).enable();
    }

    public boolean isReplicaEnabled(int nodeId) {
        return replica(nodeId).enabled();
    }

    public long nextVersion() {
        return versionCounter.incrementAndGet();
    }

    public List<Integer> replicaIdsForKey(String key) {
        return replicaSelector.select(key, replicaCount);
    }

    public TadzhnahalReplicaRecord readRecord(int nodeId, String key) throws IOException {
        if (!isReplicaEnabled(nodeId)) {
            throw new IOException("Replica is disabled");
        }

        byte[] rawValue = replica(nodeId).getRaw(key);
        if (rawValue == null) {
            return null;
        }

        return replicaRecordCodec.decode(rawValue);
    }

    public void writeRecord(int nodeId, String key, TadzhnahalReplicaRecord record) throws IOException {
        if (!isReplicaEnabled(nodeId)) {
            throw new IOException("Replica is disabled");
        }

        byte[] rawValue = replicaRecordCodec.encode(record);
        replica(nodeId).upsertRaw(key, rawValue);
    }

    public void writeTombstone(int nodeId, String key, long version) throws IOException {
        writeRecord(nodeId, key, new TadzhnahalReplicaRecord(new byte[0], version, true));
    }

    public TadzhnahalReplicaRecord latestRecord(List<TadzhnahalReplicaRecord> records) {
        return records.stream()
                .filter(Objects::nonNull)
                .max(
                        Comparator.comparingLong(TadzhnahalReplicaRecord::version)
                                .thenComparing(record -> record.deleted() ? 1 : 0)
                )
                .orElse(null);
    }

    public List<TadzhnahalReplicaNode> replicaNodes() {
        return List.copyOf(replicaNodes);
    }

    private TadzhnahalReplicaNode replica(int nodeId) {
        if (nodeId < 0 || nodeId >= replicaNodes.size()) {
            throw new IllegalArgumentException("Unknown replica id: " + nodeId);
        }

        return replicaNodes.get(nodeId);
    }

    private static List<TadzhnahalReplicaNode> createReplicaNodes(Path rootDir, int replicaCount)
            throws IOException {
        List<TadzhnahalReplicaNode> nodes = new ArrayList<>(replicaCount);

        for (int nodeId = 0; nodeId < replicaCount; nodeId++) {
            Path replicaDir = rootDir.resolve("replica-" + nodeId);
            FileDao dao = new FileDao(replicaDir);
            nodes.add(new TadzhnahalReplicaNode(nodeId, dao));
        }

        return List.copyOf(nodes);
    }
}
