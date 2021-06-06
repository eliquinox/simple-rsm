package rsm.common;

import io.aeron.ChannelUriStringBuilder;
import io.aeron.CommonContext;
import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.ConsensusModule;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.MinMulticastFlowControlSupplier;
import io.aeron.driver.ThreadingMode;
import org.agrona.ErrorHandler;
import org.agrona.IoUtil;
import org.agrona.concurrent.NoOpLock;

import java.io.File;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class ClusterNodeConfig
{
    public static final int PORT_BASE = 9000;
    public static final int PORTS_PER_NODE = 100;
    public static final int ARCHIVE_CONTROL_PORT_OFFSET = 1;
    public static final int CLIENT_FACING_PORT_OFFSET = 2;
    public static final int MEMBER_FACING_PORT_OFFSET = 3;
    public static final int LOG_PORT_OFFSET = 4;
    public static final int TRANSFER_PORT_OFFSET = 5;

    private final MediaDriver.Context mediaDriverContext;
    private final Archive.Context archiveContext;
    private final AeronArchive.Context aeronArchiveContext;
    private final ConsensusModule.Context consensusModuleContext;
    private final ClusteredServiceContainer.Context clusteredServiceContext;

    ClusterNodeConfig(
            final MediaDriver.Context mediaDriverContext,
            final Archive.Context archiveContext,
            final AeronArchive.Context aeronArchiveContext,
            final ConsensusModule.Context consensusModuleContext,
            final ClusteredServiceContainer.Context clusteredServiceContext)
    {
        this.mediaDriverContext = mediaDriverContext;
        this.archiveContext = archiveContext;
        this.aeronArchiveContext = aeronArchiveContext;
        this.consensusModuleContext = consensusModuleContext;
        this.clusteredServiceContext = clusteredServiceContext;
    }

    public static ClusterNodeConfig create(
            final int nodeId,
            final List<String> clusterHostnames,
            final ClusteredService clusteredService)
    {
        final String aeronDir = shmDirForName("rsm-cluster-node-" + nodeId);
        final String baseDir = shmDirForName("rsm-cluster-driver-" + nodeId);
        final String nodeHostname = clusterHostnames.get(nodeId);

        final String archiveControlChannel = udpChannel(nodeId, nodeHostname, ARCHIVE_CONTROL_PORT_OFFSET);
        final String archiveLogControlChannel = "aeron:ipc?term-length=64k";
        final String ingressChannel = "aeron:udp?term-length=64k";
        final String consensusModuleLogChannel = logControlChannel(nodeId, nodeHostname, LOG_PORT_OFFSET);

        final MediaDriver.Context mediaDriverContext = new MediaDriver.Context();
        final ConsensusModule.Context consensusModuleContext = new ConsensusModule.Context();
        final Archive.Context archiveContext = new Archive.Context();
        final AeronArchive.Context aeronArchiveContext = new AeronArchive.Context();
        final ClusteredServiceContainer.Context serviceContainerContext = new ClusteredServiceContainer.Context();

        mediaDriverContext
                .aeronDirectoryName(aeronDir)
                .threadingMode(ThreadingMode.SHARED)
                .termBufferSparseFile(true)
                .multicastFlowControlSupplier(new MinMulticastFlowControlSupplier())
                .dirDeleteOnStart(true);

        archiveContext
                .archiveDir(new File(baseDir, "archive"))
                .controlChannel(archiveControlChannel)
                .localControlChannel(archiveLogControlChannel)
                .recordingEventsEnabled(false)
                .threadingMode(ArchiveThreadingMode.SHARED);

        aeronArchiveContext
                .lock(NoOpLock.INSTANCE)
                .controlRequestChannel(archiveContext.controlChannel())
                .controlRequestStreamId(archiveContext.controlStreamId())
                .controlResponseChannel(archiveContext.controlChannel())
                .aeronDirectoryName(aeronDir);

        final File clusterDir = new File(baseDir, "consensus-module");
        final String clusterMembers = clusterMembers(clusterHostnames);

        consensusModuleContext
                .sessionTimeoutNs(TimeUnit.MINUTES.toNanos(60))
                .errorHandler(Throwable::printStackTrace)
                .clusterMemberId(nodeId)
                .clusterMembers(clusterMembers)
                .aeronDirectoryName(aeronDir)
                .clusterDir(clusterDir)
                .ingressChannel(ingressChannel)
                .logChannel(consensusModuleLogChannel)
                .archiveContext(aeronArchiveContext.clone())
                .deleteDirOnStart(true);

        serviceContainerContext
                .aeronDirectoryName(aeronDir)
                .archiveContext(aeronArchiveContext.clone())
                .clusterDir(new File(baseDir, "service"))
                .clusteredService(clusteredService)
                .errorHandler(Throwable::printStackTrace);

        return new ClusterNodeConfig(mediaDriverContext, archiveContext, aeronArchiveContext, consensusModuleContext, serviceContainerContext);
    }

    /**
     * Set the same error handler for all contexts.
     *
     * @param errorHandler to receive errors.
     */
    public void errorHandler(final ErrorHandler errorHandler)
    {
        this.mediaDriverContext.errorHandler(errorHandler);
        this.archiveContext.errorHandler(errorHandler);
        this.aeronArchiveContext.errorHandler(errorHandler);
        this.consensusModuleContext.errorHandler(errorHandler);
        this.clusteredServiceContext.errorHandler(errorHandler);
    }

    /**
     * Set the aeron directory for all configuration contexts.
     *
     * @param aeronDir directory to use for aeron.
     */
    public void aeronDirectoryName(final String aeronDir)
    {
        this.mediaDriverContext.aeronDirectoryName(aeronDir);
        this.archiveContext.aeronDirectoryName(aeronDir);
        this.aeronArchiveContext.aeronDirectoryName(aeronDir);
        this.consensusModuleContext.aeronDirectoryName(aeronDir);
        this.clusteredServiceContext.aeronDirectoryName(aeronDir);
    }

    /**
     * Gets the configuration's media driver context.
     *
     * @return configured {@link io.aeron.driver.MediaDriver.Context}.
     * @see io.aeron.driver.MediaDriver.Context
     */
    public MediaDriver.Context mediaDriverContext()
    {
        return mediaDriverContext;
    }

    /**
     * Gets the configuration's archive context.
     *
     * @return configured {@link io.aeron.archive.Archive.Context}.
     * @see io.aeron.archive.Archive.Context
     */
    public Archive.Context archiveContext()
    {
        return archiveContext;
    }

    /**
     * Gets the configuration's aeron archive context.
     *
     * @return configured {@link io.aeron.archive.Archive.Context}.
     * @see io.aeron.archive.client.AeronArchive.Context
     */
    public AeronArchive.Context aeronArchiveContext()
    {
        return aeronArchiveContext;
    }

    /**
     * Gets the configuration's consensus module context.
     *
     * @return configured {@link io.aeron.cluster.ConsensusModule.Context}.
     * @see io.aeron.cluster.ConsensusModule.Context
     */
    public ConsensusModule.Context consensusModuleContext()
    {
        return consensusModuleContext;
    }

    /**
     * Gets the configuration's clustered service container context.
     *
     * @return configured {@link io.aeron.cluster.service.ClusteredServiceContainer.Context}.
     * @see io.aeron.cluster.service.ClusteredServiceContainer.Context
     */
    public ClusteredServiceContainer.Context clusteredServiceContext()
    {
        return clusteredServiceContext;
    }


    public static int calculatePort(final int nodeId, final int offset)
    {
        return PORT_BASE + (nodeId * PORTS_PER_NODE) + offset;
    }

    public static String udpChannel(final int nodeId, final String hostname, final int portOffset)
    {
        final int port = calculatePort(nodeId, portOffset);
        return new ChannelUriStringBuilder()
                .media("udp")
                .termLength(64 * 1024)
                .endpoint(hostname + ":" + port)
                .build();
    }

    public static String logControlChannel(final int nodeId, final String hostname, final int portOffset)
    {
        final int port = calculatePort(nodeId, portOffset);
        return new ChannelUriStringBuilder()
                .media("udp")
                .termLength(64 * 1024)
                .controlMode("manual")
                .controlEndpoint(hostname + ":" + port)
                .build();
    }

    public static String ingressEndpoints(final List<String> hostnames)
    {
        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < hostnames.size(); i++)
        {
            sb.append(i).append('=');
            sb.append(hostnames.get(i)).append(':').append(calculatePort(i, CLIENT_FACING_PORT_OFFSET));
            sb.append(',');
        }

        sb.setLength(sb.length() - 1);
        return sb.toString();
    }

    public static String clusterMembers(final List<String> hostnames)
    {
        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < hostnames.size(); i++)
        {
            sb.append(i);
            sb.append(',').append(hostnames.get(i)).append(':').append(calculatePort(i, CLIENT_FACING_PORT_OFFSET));
            sb.append(',').append(hostnames.get(i)).append(':').append(calculatePort(i, MEMBER_FACING_PORT_OFFSET));
            sb.append(',').append(hostnames.get(i)).append(':').append(calculatePort(i, LOG_PORT_OFFSET));
            sb.append(',').append(hostnames.get(i)).append(':').append(calculatePort(i, TRANSFER_PORT_OFFSET));
            sb.append(',').append(hostnames.get(i)).append(':').append(calculatePort(i, ARCHIVE_CONTROL_PORT_OFFSET));
            sb.append('|');
        }

        return sb.toString();
    }

    public static String shmDirForName(String name)
    {
        return "/dev/shm" + File.separator + name;
    }
}
