package company.vk.edu.distrib.compute.tadzhnahal;

import company.vk.edu.distrib.compute.KVCluster;
import company.vk.edu.distrib.compute.KVClusterFactory;

import java.util.List;

public class TadzhnahalKVClusterFactory extends KVClusterFactory {
    @Override
    protected KVCluster doCreate(List<Integer> ports) {
        return new TadzhnahalKVCluster(ports);
    }
}
