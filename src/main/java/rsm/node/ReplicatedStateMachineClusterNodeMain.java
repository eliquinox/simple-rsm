package rsm.node;

import org.agrona.concurrent.SigInt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rsm.common.ClusterNodeConfig;
import rsm.common.ClusterTopologyConfiguration;

public class ReplicatedStateMachineClusterNodeMain {

    private static final Logger log = LoggerFactory.getLogger(ReplicatedStateMachineClusterNodeMain.class);

    public static void main(String[] args) {
        final int nodeId = Integer.parseInt(args[0]);
        final String topologyConfigFile = args[1];

        final ClusterTopologyConfiguration topologyConfig = ClusterTopologyConfiguration.fromYaml(topologyConfigFile);
        final ClusterNodeConfig clusterNodeConfig = ClusterNodeConfig.create(nodeId, topologyConfig.getNodeHostnames(), new ReplicatedStateMachineClusteredService());
        final ReplicatedStateMachineClusterNode clusterNode = new ReplicatedStateMachineClusterNode(clusterNodeConfig);

        log.info("Starting node {} using topology configuration {}", nodeId, topologyConfigFile);

        clusterNode.start();

        log.info("Cluster node started");

        SigInt.register(clusterNode::stop);
    }
}