package rsm.client;

import org.agrona.concurrent.SigInt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rsm.common.ClusterTopologyConfiguration;

public class ReplicatedStateMachineClientMain {

    private static final Logger log = LoggerFactory.getLogger(ReplicatedStateMachineClientMain.class);

    public static void main(String[] args) {
        final String clientHostName = args[0];
        final String topologyConfigFile = args[1];
        final ClusterTopologyConfiguration topologyConfig = ClusterTopologyConfiguration.fromYaml(topologyConfigFile);

        final ReplicatedStateMachineClient client = new ReplicatedStateMachineClient(clientHostName, topologyConfig.getNodeHostnames());

        log.info("Starting client using topology configuration {}", topologyConfigFile);

        client.start();

        log.info("Client started");

        client.setValue(100);

        final long value = client.getValue();

        log.info("Set value to {}", value);

        SigInt.register(client::stop);
    }
}
