package rsm.client;

import org.junit.jupiter.api.Test;
import rsm.common.ClusterNodeConfig;
import rsm.node.ReplicatedStateMachineClusterNode;
import rsm.node.ReplicatedStateMachineClusteredService;

import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ReplicatedStateMachineClientTest {

    private static final Supplier<ClusterNodeConfig> SINGLE_NODE_CONFIG = () -> ClusterNodeConfig.create(
            0,
            Collections.singletonList("localhost"),
            new ReplicatedStateMachineClusteredService());

    public static final String LOCALHOST = "localhost";

    @Test
    void shouldStartAndStopTheClientAndNode() {
        final ReplicatedStateMachineClusterNode clusterNode = new ReplicatedStateMachineClusterNode(SINGLE_NODE_CONFIG.get());
        final ReplicatedStateMachineClient client = new ReplicatedStateMachineClient(LOCALHOST, List.of(LOCALHOST));

        clusterNode.start();
        client.start();

        clusterNode.stop();
        client.stop();
    }

    @Test
    void shouldGetZeroInitialValue() {
        final ReplicatedStateMachineClusterNode clusterNode = new ReplicatedStateMachineClusterNode(SINGLE_NODE_CONFIG.get());
        final ReplicatedStateMachineClient client = new ReplicatedStateMachineClient(LOCALHOST, List.of(LOCALHOST));

        clusterNode.start();
        client.start();

        final long actualValue = client.getValue();

        assertEquals(0L, actualValue);

        clusterNode.stop();
        client.stop();
    }

    @Test
    void shouldSetAndGetValue() {
        final ReplicatedStateMachineClusterNode clusterNode = new ReplicatedStateMachineClusterNode(SINGLE_NODE_CONFIG.get());
        final ReplicatedStateMachineClient client = new ReplicatedStateMachineClient(LOCALHOST, List.of(LOCALHOST));

        clusterNode.start();
        client.start();

        final long newSetValue = client.setValue(101L);
        final long newGetValue = client.getValue();

        assertEquals(101L, newGetValue);
        assertEquals(newSetValue, newGetValue);

        clusterNode.stop();
        client.stop();
    }
}
