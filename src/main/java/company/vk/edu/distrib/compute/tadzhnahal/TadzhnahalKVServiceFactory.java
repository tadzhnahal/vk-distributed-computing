package company.vk.edu.distrib.compute.tadzhnahal;

import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.KVServiceFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

public class TadzhnahalKVServiceFactory extends KVServiceFactory {
    private static final int DEFAULT_REPLICA_COUNT = 3;

    private final TadzhnahalShardingAlgorithm shardingAlgorithm;

    public TadzhnahalKVServiceFactory() {
        super();
        this.shardingAlgorithm = TadzhnahalShardingAlgorithm.RENDEZVOUS;
    }

    public TadzhnahalKVServiceFactory(TadzhnahalShardingAlgorithm shardingAlgorithm) {
        super();
        this.shardingAlgorithm = shardingAlgorithm;
    }

    @Override
    protected KVService doCreate(int port) throws IOException {
        Path rootDir = buildRootDir(port);
        return new TadzhnahalKVService(port, rootDir, DEFAULT_REPLICA_COUNT);
    }

    public KVService create(int port, List<String> clusterEndpoints) throws IOException {
        Path rootDir = buildRootDir(port);
        return new TadzhnahalKVService(
                port,
                rootDir,
                DEFAULT_REPLICA_COUNT,
                clusterEndpoints
        );
    }

    public KVService create(
            int port,
            int grpcPort,
            List<String> clusterEndpoints
    ) throws IOException {
        Path rootDir = buildRootDir(port);
        return new TadzhnahalKVService(
                port,
                rootDir,
                DEFAULT_REPLICA_COUNT,
                grpcPort,
                clusterEndpoints
        );
    }

    public TadzhnahalShardingAlgorithm shardingAlgorithm() {
        return shardingAlgorithm;
    }

    private Path buildRootDir(int port) {
        return Path.of(
                System.getProperty("user.home"),
                ".vk-distributed-computing",
                "tadzhnahal",
                "port-" + port
        );
    }
}
