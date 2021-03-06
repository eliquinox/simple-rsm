package rsm.client;

import io.aeron.ChannelUriStringBuilder;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.EgressListener;
import io.aeron.cluster.codecs.EventCode;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.Header;
import org.agrona.BitUtil;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableDirectByteBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.SleepingMillisIdleStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rsm.common.ClusterNodeConfig;
import rsm.node.MessageType;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static io.aeron.CommonContext.UDP_MEDIA;
import static org.awaitility.Awaitility.await;

public class ReplicatedStateMachineClient implements EgressListener {

    private static final Logger log = LoggerFactory.getLogger(ReplicatedStateMachineClient.class);

    private final List<String> clusterNodeHostnames;
    private MediaDriver mediaDriver;
    private AeronCluster clusterClient;
    private final IdleStrategy idleStrategy = new SleepingMillisIdleStrategy();
    private final AtomicLong lastReceivedCorrelationId = new AtomicLong(0L);
    private final AtomicLong lastReceivedValue = new AtomicLong(0L);
    private final AtomicInteger lastReplyingNodeId = new AtomicInteger(-1);
    private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
    private final String clientHostName;

    public ReplicatedStateMachineClient(final String clientHostName, final List<String> clusterNodeHostnames) {
        this.clientHostName = clientHostName;
        this.clusterNodeHostnames = clusterNodeHostnames;
    }

    public void start() {
        final String ingressEndpoints = ClusterNodeConfig.ingressEndpoints(clusterNodeHostnames);

        this.mediaDriver = MediaDriver.launchEmbedded(new MediaDriver.Context()
                .aeronDirectoryName(ClusterNodeConfig.shmDirForName("rsm-client"))
                .errorHandler(Throwable::printStackTrace)
                .threadingMode(ThreadingMode.SHARED)
                .dirDeleteOnStart(true)
                .dirDeleteOnShutdown(true));

        final String egressChannel = new ChannelUriStringBuilder()
                .media(UDP_MEDIA)
                .endpoint(clientHostName + ":" + 19001)
                .build();

        this.clusterClient = AeronCluster.connect(
                new AeronCluster.Context()
                        .messageTimeoutNs(TimeUnit.SECONDS.toNanos(30))
                        .egressListener(this)
                        .egressChannel(egressChannel)
                        .aeronDirectoryName(mediaDriver.aeronDirectoryName())
                        .ingressEndpoints(ingressEndpoints));

        await().until(() -> clusterClient.egressSubscription().isConnected());

        this.executor.scheduleAtFixedRate(clusterClient::pollEgress, 0L, 10L, TimeUnit.NANOSECONDS);
        this.executor.scheduleAtFixedRate(clusterClient::sendKeepAlive, 0L, 1_000_000L, TimeUnit.NANOSECONDS);
    }

    public void stop() {
        clusterClient.close();
        mediaDriver.close();
    }

    public long getValue() {
        final MutableDirectBuffer buffer = new ExpandableDirectByteBuffer();
        final long correlationId = System.nanoTime();

        log.info("Sending GET value request with correlation ID: {}", correlationId);

        buffer.putLong(0, correlationId);
        buffer.putChar(BitUtil.SIZE_OF_LONG, MessageType.GET.getCharCode());

        offer(buffer, buffer.capacity());

        waitForCorrelationId(correlationId);

        return lastReceivedValue.get();
    }

    public long setValue(final long value) {
        final MutableDirectBuffer buffer = new ExpandableDirectByteBuffer();
        final long correlationId = System.nanoTime();

        log.info("Sending SET value request with correlation ID: {}", correlationId);

        buffer.putLong(0, correlationId);
        buffer.putChar(BitUtil.SIZE_OF_LONG, MessageType.SET.getCharCode());
        buffer.putLong(BitUtil.SIZE_OF_LONG * 2, value);

        offer(buffer, buffer.capacity());

        waitForCorrelationId(correlationId);

        return lastReceivedValue.get();
    }

    private void waitForCorrelationId(final long correlationId) {
        while (lastReceivedCorrelationId.get() != correlationId) {
            idleStrategy.idle(clusterClient.pollEgress());
        }
    }

    @Override
    public void onMessage(final long clusterSessionId,
                          final long timestamp,
                          final DirectBuffer buffer,
                          final int offset,
                          final int length,
                          final Header header) {

        final long correlationId = buffer.getLong(offset);
        final long value = buffer.getLong(offset + BitUtil.SIZE_OF_LONG);
        final int nodeId = buffer.getInt(offset + BitUtil.SIZE_OF_LONG * 2);

        log.info("Received message with correlation ID: {} and value: {}", correlationId, value);

        lastReceivedCorrelationId.set(correlationId);
        lastReceivedValue.set(value);
        lastReplyingNodeId.set(nodeId);
    }

    @Override
    public void onSessionEvent(final long correlationId,
                               final long clusterSessionId,
                               final long leadershipTermId,
                               final int leaderMemberId,
                               final EventCode code,
                               final String detail) {
        log.info("Received session event with code: {}", code);
    }

    @Override
    public void onNewLeader(final long clusterSessionId, final long leadershipTermId, final int leaderMemberId, final String ingressEndpoints) {
        log.info("Received onNewLeader event. New leader: {}", leaderMemberId);
    }

    private void offer(final MutableDirectBuffer buffer, final int length)
    {
        long result;
        do
        {
            result = clusterClient.offer(buffer, 0, length);
            if (result < 0)
            {
                log.info("Potentially unexpected offer result on client side: {}", result);
            }
            idleStrategy.idle((int) result);
            clusterClient.pollEgress();
        }
        while (result < 0);

        log.info("Offered. Result: {}", result);
    }

    public int getLastReplyingNodeId() {
        return lastReplyingNodeId.get();
    }
}
