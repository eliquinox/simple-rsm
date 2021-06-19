package rsm.node;

import io.aeron.cluster.ClusterTool;
import io.aeron.cluster.service.Cluster;
import org.agrona.collections.MutableReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import rsm.client.ReplicatedStateMachineClient;
import rsm.common.ClusterNodeConfig;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class ReplicatedStateMachineMultiNodeTest {

    public static final String LOCALHOST = "localhost";
    private ReplicatedStateMachineClusterNode node1;
    private ReplicatedStateMachineClusterNode node2;
    private ReplicatedStateMachineClusterNode node3;
    private ReplicatedStateMachineClusterNode node4;
    public static final List<String> CLUSTER_NODE_HOSTNAMES = List.of(LOCALHOST, LOCALHOST, LOCALHOST, LOCALHOST);
    private ReplicatedStateMachineClient client;

    @BeforeEach
    void setUp() {
        final ClusterNodeConfig node1Config = ClusterNodeConfig.create(
                0,
                CLUSTER_NODE_HOSTNAMES,
                new ReplicatedStateMachineClusteredService());

        final ClusterNodeConfig node2Config = ClusterNodeConfig.create(
                1,
                CLUSTER_NODE_HOSTNAMES,
                new ReplicatedStateMachineClusteredService());

        final ClusterNodeConfig node3Config = ClusterNodeConfig.create(
                2,
                CLUSTER_NODE_HOSTNAMES,
                new ReplicatedStateMachineClusteredService());

        final ClusterNodeConfig node4Config = ClusterNodeConfig.create(
                3,
                CLUSTER_NODE_HOSTNAMES,
                new ReplicatedStateMachineClusteredService());

        node1 = new ReplicatedStateMachineClusterNode(node1Config);
        node2 = new ReplicatedStateMachineClusterNode(node2Config);
        node3 = new ReplicatedStateMachineClusterNode(node3Config);
        node4 = new ReplicatedStateMachineClusterNode(node4Config);

        node1.start();
        node2.start();
        node3.start();
        node4.start();

        awaitLeader(node1, node2, node3, node4);

        client = new ReplicatedStateMachineClient(LOCALHOST, CLUSTER_NODE_HOSTNAMES);
        client.start();
    }

    @AfterEach
    void tearDown() {
        node1.stop();
        node2.stop();
        node3.stop();
        node4.stop();

        client.stop();
    }

    @Test
    void shouldOperateMultiNodeCluster() {
        client.setValue(101L);

        final long actualValue = client.getValue();

        assertEquals(101L, actualValue);
    }

    @Test
    void shouldContinueToOperateAfterLeaderIsRemoved() {
        client.setValue(101L);

        final long actualValue = client.getValue();

        assertEquals(101L, actualValue);

        final ReplicatedStateMachineClusterNode leader = awaitLeader(node1, node2, node3, node4);

        ClusterTool.removeMember(leader.getClusterDir(), leader.getClusterMemberId(), false);

        awaitLeader(Stream.of(node1, node2, node3, node4)
                .filter(node -> node != leader)
                .toArray(ReplicatedStateMachineClusterNode[]::new));

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
                    System.out.println("Current cluster role view: " + Arrays.stream(clusterNodes).map(ReplicatedStateMachineClusterNode::getRole).toList());

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
