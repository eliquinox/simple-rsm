package rsm.gateway;

import lombok.extern.slf4j.Slf4j;
import org.agrona.concurrent.SigInt;
import rsm.client.ReplicatedStateMachineClient;
import rsm.common.ClusterTopologyConfiguration;

import static spark.route.HttpMethod.get;

@Slf4j
public class HttpGatewayMain {

    public static void main(String[] args) {
        final String topologyConfigFile = args[0];
        final ClusterTopologyConfiguration topologyConfig = ClusterTopologyConfiguration.fromYaml(topologyConfigFile);

        final ReplicatedStateMachineClient client = new ReplicatedStateMachineClient(topologyConfig.getNodeHostnames());

        log.info("Starting client using topology configuration {}", topologyConfigFile);

        final HttpGateway httpGateway = new HttpGateway(client);

        httpGateway.start();


        SigInt.register(() -> {
            httpGateway.close();
            client.stop();
        });
    }
}
