package rsm.node;

import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.cluster.client.ClusterException;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.logbuffer.Header;
import org.agrona.BitUtil;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class ReplicatedStateMachineClusteredService implements ClusteredService {

    private static final Logger log = LoggerFactory.getLogger(ReplicatedStateMachineClusteredService.class);
    public static final int MESSAGE_TYPE_POSITION = BitUtil.SIZE_OF_LONG;
    public static final int VALUE_POSITION = BitUtil.SIZE_OF_LONG * 2;

    private final ReplicatedStateMachine replicatedStateMachine = new ReplicatedStateMachine();
    private final MutableDirectBuffer responseBuffer = new ExpandableArrayBuffer();
    private Cluster cluster;

    @Override
    public void onStart(final Cluster cluster, final Image snapshotImage) {
        this.cluster = cluster;
    }

    @Override
    public void onSessionOpen(final ClientSession session, final long timestamp) {
        log.info("Session {} opened at {}", session.id(), timestamp);
    }

    @Override
    public void onSessionClose(final ClientSession session, final long timestamp, final CloseReason closeReason) {
        log.info("Session {} closed at {} of {}", session.id(), timestamp, closeReason);
    }

    @Override
    public void onSessionMessage(final ClientSession session,
                                 final long timestamp,
                                 final DirectBuffer buffer,
                                 final int offset,
                                 final int length,
                                 final Header header) {

        final long correlationId = buffer.getLong(offset);
        final MessageType messageType = MessageType.fromCode(buffer.getChar(offset + MESSAGE_TYPE_POSITION));

        log.info("Cluster node {} received request with correlation ID: {} and type: {}", cluster.memberId(), correlationId, messageType);

        switch (messageType)
        {
            case GET -> {
            }
            case SET -> {
                final long valueToSet = buffer.getLong(offset + VALUE_POSITION);
                replicatedStateMachine.setValue(valueToSet);
            }
            default -> throw new IllegalArgumentException("Unexpected message type: " + messageType);
        }

        responseBuffer.putLong(0, correlationId);
        responseBuffer.putLong(BitUtil.SIZE_OF_LONG, replicatedStateMachine.getValue());
        responseBuffer.putInt(BitUtil.SIZE_OF_LONG * 2, cluster.memberId());

        session.offer(responseBuffer, 0, responseBuffer.capacity());
    }

    @Override
    public void onTimerEvent(final long correlationId, final long timestamp) {

    }

    @Override
    public void onTakeSnapshot(final ExclusivePublication snapshotPublication) {

    }

    @Override
    public void onRoleChange(final Cluster.Role newRole) {
        log.info("Cluster node {} has a new role: {}", cluster.memberId(), newRole);
    }

    @Override
    public void onTerminate(final Cluster cluster) {
        log.info("Cluster node {} is in onTermination. It's role is {}", cluster.memberId(), cluster.role());
    }

    public Cluster.Role getRole()
    {
        return cluster.role();
    }

    public int getMemberId() {
        return cluster.memberId();
    }
}
