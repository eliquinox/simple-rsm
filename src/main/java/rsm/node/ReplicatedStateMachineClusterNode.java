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

public class ReplicatedStateMachineClusterNode {

    private static final Logger log = LoggerFactory.getLogger(ReplicatedStateMachineClusterNode.class);
    private final ClusterNodeConfig clusterNodeConfig;
    private ClusteredMediaDriver clusteredMediaDriver;
    private ClusteredServiceContainer serviceContainer;


    public ReplicatedStateMachineClusterNode(final ClusterNodeConfig clusterNodeConfig) {
        this.clusterNodeConfig = clusterNodeConfig;
    }

    public void start() {
        clusterNodeConfig.mediaDriverContext().errorHandler(errorHandler("Media Driver"));
        clusterNodeConfig.archiveContext().deleteArchiveOnStart(true).errorHandler(errorHandler("Archive"));
        clusterNodeConfig.aeronArchiveContext().errorHandler(errorHandler("Aeron Archive"));
        clusterNodeConfig.consensusModuleContext().deleteDirOnStart(true).errorHandler(errorHandler("Consensus Module"));
        clusterNodeConfig.clusteredServiceContext().errorHandler(errorHandler("Clustered Service"));

        this.clusteredMediaDriver = ClusteredMediaDriver.launch(
                clusterNodeConfig.mediaDriverContext(),
                clusterNodeConfig.archiveContext(),
                clusterNodeConfig.consensusModuleContext()
                        .sessionTimeoutNs(TimeUnit.MINUTES.toNanos(1)));

        this.serviceContainer = ClusteredServiceContainer.launch(
                clusterNodeConfig.clusteredServiceContext());
    }

    public void stop() {
        serviceContainer.close();
        clusteredMediaDriver.close();
    }

    private ErrorHandler errorHandler(final String context)
    {
        return (Throwable throwable) ->
        {
            log.error(context + " - " + getClusterMemberId());
            throwable.printStackTrace(System.err);
        };
    }

    public int getClusterMemberId() {
        return clusterNodeConfig.consensusModuleContext().clusterMemberId();
    }

    public Cluster.Role getRole() {
        return getService().getRole();
    }

    public File getClusterDir() {
        return clusterNodeConfig.consensusModuleContext().clusterDir();
    }

    private ReplicatedStateMachineClusteredService getService() {
        return (ReplicatedStateMachineClusteredService) clusterNodeConfig.clusteredServiceContext().clusteredService();
    }
}