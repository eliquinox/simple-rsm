package rsm.node;

import io.aeron.samples.cluster.ClusterConfig;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.function.Supplier;

class ReplicatedStateMachineClusteredServiceTest {

    private static final Supplier<ClusterConfig> SINGLE_NODE_CONFIG = () -> ClusterConfig.create(
            0,
            Collections.singletonList("localhost"),
            Collections.singletonList("localhost"),
            9000,
            new ReplicatedStateMachineClusteredService());

    @Test
    void shouldStartAndStopClusterNode() {
        final ReplicatedStateMachineClusterNode clusterNode =
                new ReplicatedStateMachineClusterNode(SINGLE_NODE_CONFIG.get());

        clusterNode.start();
        clusterNode.stop();
    }
}