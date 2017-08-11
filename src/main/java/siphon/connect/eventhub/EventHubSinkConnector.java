package siphon.connect.eventhub;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class EventHubSinkConnector extends SinkConnector {

    public static final String EVENTHUB_NAME = "eventhub_name";
    public static final String CONNECTION_STRING = "connection_string";
    public static final String PARTITION_KEY = "partition_key";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(EVENTHUB_NAME, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Name of the EventHub sending to")
            .define(CONNECTION_STRING, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Connection string of the EventHub")
            .define(PARTITION_KEY, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Partition key")
            ;

    private Map<String, Object> configProperties;

    @Override
    public String version() {
        return new EventHubSinkConnector().version();
    }

    @Override
    public void start(Map<String, String> props) {
        configProperties = CONFIG_DEF.parse(props);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return EventHubSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        ArrayList<Map<String, String>> configs = new ArrayList<Map<String, String>>();
        for (int i = 0; i < maxTasks; i++) {
            Map<String, String> props = new HashMap<String, String>();
            props.put(EVENTHUB_NAME, configProperties.get(EVENTHUB_NAME).toString());
            props.put(CONNECTION_STRING, configProperties.get(CONNECTION_STRING).toString());
            props.put(PARTITION_KEY, configProperties.get(PARTITION_KEY).toString());
            configs.add(props);
        }
        return configs;
    }

    @Override
    public void stop() {
        // do nothing
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }
}
