package rsm.node;

import io.aeron.cluster.ClusteredMediaDriver;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.samples.cluster.ClusterConfig;
import org.agrona.ErrorHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReplicatedStateMachineClusterNode {

    private static final Logger log = LoggerFactory.getLogger(ReplicatedStateMachineClusterNode.class);
    private final ClusterConfig clusterConfig;
    private ClusteredMediaDriver clusteredMediaDriver;
    private ClusteredServiceContainer serviceContainer;


    public ReplicatedStateMachineClusterNode(final ClusterConfig clusterConfig) {
        this.clusterConfig = clusterConfig;
    }

    public void start() {
        clusterConfig.mediaDriverContext().errorHandler(errorHandler("Media Driver"));
        clusterConfig.archiveContext().deleteArchiveOnStart(true).errorHandler(errorHandler("Archive"));
        clusterConfig.aeronArchiveContext().errorHandler(errorHandler("Aeron Archive"));
        clusterConfig.consensusModuleContext().deleteDirOnStart(true).errorHandler(errorHandler("Consensus Module"));
        clusterConfig.clusteredServiceContext().errorHandler(errorHandler("Clustered Service"));

        this.clusteredMediaDriver = ClusteredMediaDriver.launch(
                clusterConfig.mediaDriverContext()
                        .dirDeleteOnShutdown(true)
                        .dirDeleteOnStart(true),
                clusterConfig.archiveContext(),
                clusterConfig.consensusModuleContext()
                        .deleteDirOnStart(true));
        this.serviceContainer = ClusteredServiceContainer.launch(
                clusterConfig.clusteredServiceContext());
    }

    public void stop() {
        serviceContainer.close();
        clusteredMediaDriver.close();
    }

    private static ErrorHandler errorHandler(final String context)
    {
        return (Throwable throwable) ->
        {
            log.error(context);
            throwable.printStackTrace(System.err);
        };
    }

    public Cluster.Role getRole() {
        return ((ReplicatedStateMachineClusteredService) clusterConfig.clusteredServiceContext().clusteredService()).getRole();
    }
}