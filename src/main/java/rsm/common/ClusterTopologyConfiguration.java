package rsm.common;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import lombok.Data;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Data
public class ClusterTopologyConfiguration
{
    private Map<Integer, String> nodes;

    public static ClusterTopologyConfiguration fromYaml(final String fileName)
    {
        try
        {
            final ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory()).findAndRegisterModules();
            final URL yamlResource = ClassLoader.getSystemResource(fileName);
            return objectMapper.readValue(yamlResource, ClusterTopologyConfiguration.class);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    public Map<Integer, String> getNodes() {
        return new HashMap<>(nodes);
    }

    public List<String> getNodeHostnames() {
        return new ArrayList<>(nodes.values());
    }
}