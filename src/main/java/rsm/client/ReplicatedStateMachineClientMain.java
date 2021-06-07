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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

import static io.aeron.CommonContext.UDP_MEDIA;
import static org.awaitility.Awaitility.await;

public class ReplicatedStateMachineClientMain {

    private static final Logger log = LoggerFactory.getLogger(ReplicatedStateMachineClientMain.class);

    public static void main(String[] args) {

    }
}
