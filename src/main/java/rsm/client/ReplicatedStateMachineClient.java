package rsm.client;

import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.EgressListener;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.Header;
import io.aeron.samples.cluster.ClusterConfig;
import rsm.node.MessageType;
import org.agrona.BitUtil;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableDirectByteBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.SleepingMillisIdleStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.awaitility.Awaitility.await;

public class ReplicatedStateMachineClient implements EgressListener {

    private static final Logger log = LoggerFactory.getLogger(ReplicatedStateMachineClient.class);
    public static final int CLIENT_FACING_OFFSET = 10;

    private final int port;
    private final List<String> clusterNodeHostnames;
    private MediaDriver mediaDriver;
    private AeronCluster clusterClient;
    private final IdleStrategy idleStrategy = new SleepingMillisIdleStrategy();
    private final AtomicLong lastReceivedCorrelationId = new AtomicLong(0L);
    private final AtomicLong lastReceivedValue = new AtomicLong(0L);

    public ReplicatedStateMachineClient(final List<String> clusterNodeHostnames, final int port) {
        this.clusterNodeHostnames = clusterNodeHostnames;
        this.port = port;
    }

    public void start() {
        final String ingressEndpoints = ingressEndpoints(clusterNodeHostnames);

        this.mediaDriver = MediaDriver.launchEmbedded(new MediaDriver.Context()
                .threadingMode(ThreadingMode.SHARED)
                .dirDeleteOnStart(true)
                .dirDeleteOnShutdown(true));

        this.clusterClient = AeronCluster.connect(
                new AeronCluster.Context()
                        .egressListener(this)
                        .egressChannel("aeron:udp?endpoint=localhost:0")
                        .aeronDirectoryName(mediaDriver.aeronDirectoryName())
                        .ingressChannel("aeron:udp")
                        .ingressEndpoints(ingressEndpoints));
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

        log.info("Received message with correlation ID: {} and value: {}", correlationId, value);

        lastReceivedCorrelationId.set(correlationId);
        lastReceivedValue.set(value);
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
        }
        while (result < 0);
    }

    private String ingressEndpoints(final List<String> hostnames)
    {
        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < hostnames.size(); i++)
        {
            sb.append(i).append('=');
            sb.append(hostnames.get(i)).append(':').append(calculatePort(i));
            sb.append(',');
        }

        sb.setLength(sb.length() - 1);
        return sb.toString();
    }

    private int calculatePort(final int nodeId) {
        return port + (nodeId * ClusterConfig.PORTS_PER_NODE) + CLIENT_FACING_OFFSET;
    }
}
