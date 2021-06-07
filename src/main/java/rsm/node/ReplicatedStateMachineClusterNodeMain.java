package rsm.node;

import io.aeron.cluster.ClusteredMediaDriver;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusteredServiceContainer;
import org.agrona.ErrorHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rsm.common.ClusterNodeConfig;

import java.io.File;
import java.util.concurrent.TimeUnit;

public class ReplicatedStateMachineClusterNodeMain {

    private static final Logger log = LoggerFactory.getLogger(ReplicatedStateMachineClusterNodeMain.class);

    public static void main(String[] args) {
    }
}