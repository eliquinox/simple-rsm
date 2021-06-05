package rsm.client;

import io.aeron.samples.cluster.ClusterConfig;
import rsm.node.ReplicatedStateMachineClusterNode;
import rsm.node.ReplicatedStateMachineClusteredService;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ReplicatedStateMachineClientTest {

    private static final Supplier<ClusterConfig> SINGLE_NODE_CONFIG = () -> ClusterConfig.create(
            0,
            Collections.singletonList("localhost"),
            Collections.singletonList("localhost"),
            9000,
            new ReplicatedStateMachineClusteredService());

    public static final String LOCALHOST = "localhost";

    @Test
    void shouldStartAndStopTheClientAndNode() {
        final ReplicatedStateMachineClusterNode clusterNode = new ReplicatedStateMachineClusterNode(SINGLE_NODE_CONFIG.get());
        final ReplicatedStateMachineClient client = new ReplicatedStateMachineClient(List.of(LOCALHOST), 9000);

        clusterNode.start();
        client.start();

        clusterNode.stop();
        client.stop();
    }

    @Test
    void shouldGetZeroInitialValue() {
        final ReplicatedStateMachineClusterNode clusterNode = new ReplicatedStateMachineClusterNode(SINGLE_NODE_CONFIG.get());
        final ReplicatedStateMachineClient client = new ReplicatedStateMachineClient(List.of(LOCALHOST), 9000);

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
        final ReplicatedStateMachineClient client = new ReplicatedStateMachineClient(List.of(LOCALHOST), 9000);

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
