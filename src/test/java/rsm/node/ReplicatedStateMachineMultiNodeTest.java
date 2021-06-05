package rsm.node;

import io.aeron.cluster.ClusterTool;
import org.agrona.collections.MutableReference;
import rsm.client.ReplicatedStateMachineClient;
import io.aeron.cluster.service.Cluster;
import io.aeron.samples.cluster.ClusterConfig;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Supplier;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;

public class ReplicatedStateMachineMultiNodeTest {

    public static final String LOCALHOST = "localhost";
    public static final int PORT_BASE = 9000;

    @Test
    void shouldOperateMultiNodeCluster() {
        final List<String> clusterNodeHostnames = List.of(LOCALHOST, LOCALHOST, LOCALHOST);
        final ClusterConfig node1Config = ClusterConfig.create(
                0,
                clusterNodeHostnames,
                clusterNodeHostnames,
                PORT_BASE,
                new ReplicatedStateMachineClusteredService());

        final ClusterConfig node2Config = ClusterConfig.create(
                1,
                clusterNodeHostnames,
                clusterNodeHostnames,
                PORT_BASE,
                new ReplicatedStateMachineClusteredService());

        final ClusterConfig node3Config = ClusterConfig.create(
                2,
                clusterNodeHostnames,
                clusterNodeHostnames,
                PORT_BASE,
                new ReplicatedStateMachineClusteredService());

        final ReplicatedStateMachineClusterNode node1 = new ReplicatedStateMachineClusterNode(node1Config);
        final ReplicatedStateMachineClusterNode node2 = new ReplicatedStateMachineClusterNode(node2Config);
        final ReplicatedStateMachineClusterNode node3 = new ReplicatedStateMachineClusterNode(node3Config);

        node1.start();
        node2.start();
        node3.start();

        awaitLeader(node1, node2, node3);

        final ReplicatedStateMachineClient client = new ReplicatedStateMachineClient(clusterNodeHostnames, PORT_BASE);

        client.start();

        client.setValue(101L);

        final long actualValue = client.getValue();

        assertEquals(101L, actualValue);
    }

    @Test
    void shouldContinueToOperateAfterLeaderIsStopped() {
        final List<String> clusterNodeHostnames = List.of(LOCALHOST, LOCALHOST, LOCALHOST, LOCALHOST);
        final ClusterConfig node1Config = ClusterConfig.create(
                0,
                clusterNodeHostnames,
                clusterNodeHostnames,
                PORT_BASE,
                new ReplicatedStateMachineClusteredService());

        final ClusterConfig node2Config = ClusterConfig.create(
                1,
                clusterNodeHostnames,
                clusterNodeHostnames,
                PORT_BASE,
                new ReplicatedStateMachineClusteredService());

        final ClusterConfig node3Config = ClusterConfig.create(
                2,
                clusterNodeHostnames,
                clusterNodeHostnames,
                PORT_BASE,
                new ReplicatedStateMachineClusteredService());

        final ClusterConfig node4Config = ClusterConfig.create(
                3,
                clusterNodeHostnames,
                clusterNodeHostnames,
                PORT_BASE,
                new ReplicatedStateMachineClusteredService());

        final ReplicatedStateMachineClusterNode node1 = new ReplicatedStateMachineClusterNode(node1Config);
        final ReplicatedStateMachineClusterNode node2 = new ReplicatedStateMachineClusterNode(node2Config);
        final ReplicatedStateMachineClusterNode node3 = new ReplicatedStateMachineClusterNode(node3Config);
        final ReplicatedStateMachineClusterNode node4 = new ReplicatedStateMachineClusterNode(node4Config);

        node1.start();
        node2.start();
        node3.start();
        node4.start();

        final ReplicatedStateMachineClusterNode leader = awaitLeader(node1, node2, node3, node4);


        final ReplicatedStateMachineClient client = new ReplicatedStateMachineClient(clusterNodeHostnames, PORT_BASE);
        client.start();

        client.setValue(101L);

        final long actualValue = client.getValue();

        assertEquals(101L, actualValue);

        leader.stop();

        final ReplicatedStateMachineClusterNode newLeader = awaitLeader(node1, node2, node3, node4);

        assertNotSame(leader, newLeader);

        client.setValue(102L);

        final long actualValueNew = client.getValue();

        assertEquals(102L, actualValueNew);
    }

    private static ReplicatedStateMachineClusterNode awaitLeader(final ReplicatedStateMachineClusterNode... clusterNodes)
    {
        final MutableReference<ReplicatedStateMachineClusterNode> leaderNode = new MutableReference<>();
        await().timeout(20, TimeUnit.SECONDS)
                .pollDelay(1, TimeUnit.SECONDS)
                .then()
                .until(() -> {
                    final Optional<ReplicatedStateMachineClusterNode> maybeLeader = Arrays.stream(clusterNodes)
                            .filter(node -> node.getRole() == Cluster.Role.LEADER)
                            .findFirst();

                    if (maybeLeader.isPresent())
                    {
                        leaderNode.set(maybeLeader.get());
                        return true;
                    }

                    return false;
                });

        return leaderNode.get();
    }
}
