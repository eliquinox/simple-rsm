package rsm.node;

import org.junit.jupiter.api.Test;
import rsm.common.ClusterNodeConfig;

import java.util.Collections;
import java.util.function.Supplier;

class ReplicatedStateMachineClusteredServiceTest {

    private static final Supplier<ClusterNodeConfig> SINGLE_NODE_CONFIG = () -> ClusterNodeConfig.create(
            0,
            Collections.singletonList("localhost"),
            new ReplicatedStateMachineClusteredService());

    @Test
    void shouldStartAndStopClusterNode() {
        final ReplicatedStateMachineClusterNode clusterNode = new ReplicatedStateMachineClusterNode(SINGLE_NODE_CONFIG.get());

        clusterNode.start();
        clusterNode.stop();
    }
}