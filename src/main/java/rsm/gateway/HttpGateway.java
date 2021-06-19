package rsm.gateway;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import rsm.client.ReplicatedStateMachineClient;
import rsm.gateway.dto.ValueResponse;
import rsm.gateway.dto.SetValueRequest;
import spark.Request;
import spark.Response;

import static spark.Spark.*;


public class HttpGateway {

    private final ReplicatedStateMachineClient client;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public HttpGateway(final ReplicatedStateMachineClient client) {
        this.client = client;
    }

    public void start() {
        client.start();
        registerRoutes();
    }

    private void registerRoutes() {
        port(8080);
        get("/value", this::getValue);
        put("/value", this::setValue);
        awaitInitialization();
    }

    private Object getValue(final Request request, final Response response) throws JsonProcessingException {
        final long value = client.getValue();
        final int nodeId = client.getLastReplyingNodeId();

        final ValueResponse valueResponse = new ValueResponse();
        valueResponse.setValue(value);
        valueResponse.setServerNodeId(nodeId);

        return objectMapper.writeValueAsString(valueResponse);
    }

    private Object setValue(final Request request, final Response response) throws JsonProcessingException {
        final SetValueRequest setValueRequest = objectMapper.readValue(request.body(), SetValueRequest.class);
        final long value = client.setValue(setValueRequest.getValue());
        final int nodeId = client.getLastReplyingNodeId();

        final ValueResponse valueResponse = new ValueResponse();
        valueResponse.setValue(value);
        valueResponse.setServerNodeId(nodeId);

        return objectMapper.writeValueAsString(valueResponse);
    }

    public void close() {
        client.stop();
        stop();
    }
}
