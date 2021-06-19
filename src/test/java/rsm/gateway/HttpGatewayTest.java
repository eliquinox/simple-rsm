package rsm.gateway;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import rsm.client.ReplicatedStateMachineClient;
import rsm.common.ClusterNodeConfig;
import rsm.gateway.dto.ValueResponse;
import rsm.gateway.dto.SetValueRequest;
import rsm.node.ReplicatedStateMachineClusterNode;
import rsm.node.ReplicatedStateMachineClusteredService;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class HttpGatewayTest {

    public static final Map<Object, Object> EMPTY_BODY = Map.of();
    private final HttpClient httpClient = HttpClient.newHttpClient();
    private final ObjectMapper objectMapper = new ObjectMapper();

    private ReplicatedStateMachineClusterNode clusterNode;
    private HttpGateway httpGateway;

    public static final String LOCALHOST = "localhost";

    private static final Supplier<ClusterNodeConfig> SINGLE_NODE_CONFIG = () -> ClusterNodeConfig.create(
            0,
            Collections.singletonList(LOCALHOST),
            new ReplicatedStateMachineClusteredService());

    @BeforeEach
    public void setUp() {
        clusterNode = new ReplicatedStateMachineClusterNode(SINGLE_NODE_CONFIG.get());
        clusterNode.start();

        httpGateway = new HttpGateway(new ReplicatedStateMachineClient(LOCALHOST, List.of(LOCALHOST)));
        httpGateway.start();
    }

    @AfterEach
    void tearDown() {
        clusterNode.stop();
        httpGateway.close();
    }

    @Test
    public void shouldRetrieveTheInitialValue() throws IOException, InterruptedException {
        final ValueResponse valueResponse = getValue();
        assertEquals(0L, valueResponse.getValue());
    }

    @Test
    public void shouldReportServerNodeIdInResponse() throws IOException, InterruptedException {
        final ValueResponse valueResponse = getValue();
        assertEquals(0, valueResponse.getServerNodeId());
    }

    @Test
    public void shouldSetAndRetrieveUpdateValue() throws IOException, InterruptedException {
        final long newValue = 10101L;
        final ValueResponse valueResponse = setValue(newValue);
        assertEquals(newValue, valueResponse.getValue());
    }

    private ValueResponse setValue(long newValue) throws IOException, InterruptedException {
        final SetValueRequest setValueRequest = new SetValueRequest();
        setValueRequest.setValue(newValue);

        final HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:8080/value"))
                .method("PUT", HttpRequest.BodyPublishers.ofString(objectMapper.writeValueAsString(setValueRequest)))
                .build();

        final HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        return objectMapper.readValue(response.body(), ValueResponse.class);
    }

    private ValueResponse getValue() throws IOException, InterruptedException {
        final HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:8080/value"))
                .method("GET", HttpRequest.BodyPublishers.ofString(objectMapper.writeValueAsString(EMPTY_BODY)))
                .build();

        final HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        return objectMapper.readValue(response.body(), ValueResponse.class);
    }
}