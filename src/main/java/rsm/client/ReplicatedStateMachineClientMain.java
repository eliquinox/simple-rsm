package rsm.client;

import org.agrona.concurrent.SigInt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rsm.common.ClusterTopologyConfiguration;

public class ReplicatedStateMachineClientMain {

    private static final Logger log = LoggerFactory.getLogger(ReplicatedStateMachineClientMain.class);

    public static void main(String[] args) {
        final String topologyConfigFile = args[1];
        final ClusterTopologyConfiguration topologyConfig = ClusterTopologyConfiguration.fromYaml(topologyConfigFile);

        final ReplicatedStateMachineClient client = new ReplicatedStateMachineClient(topologyConfig.getNodeHostnames());

        log.info("Starting client using topology configuration {}", topologyConfigFile);

        client.start();

        log.info("Client started");

        SigInt.register(client::stop);
    }
}
