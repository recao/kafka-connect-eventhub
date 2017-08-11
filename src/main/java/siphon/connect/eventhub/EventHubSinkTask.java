package siphon.connect.eventhub;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;

import com.microsoft.azure.eventhubs.*;


public class EventHubSinkTask extends SinkTask {
    private static final Logger log = LoggerFactory.getLogger(EventHubSinkConnector.class);
    private String eventhub_name;
    private String connection_string;
    private String partition_key;

    private EventHubClient ehClient;

    public EventHubSinkTask() {
        // do nothing
    }

    public String version() {
        return new EventHubSinkConnector().version();
    }

    private void readConfig(Map<String, String> props) {
        Map<String, Object> config = EventHubSinkConnector.CONFIG_DEF.parse(props);
        eventhub_name = (String) config.get(EventHubSinkConnector.EVENTHUB_NAME);
        connection_string = (String) config.get(EventHubSinkConnector.CONNECTION_STRING);
        partition_key = (String) config.get(EventHubSinkConnector.PARTITION_KEY);
    }

    @Override
    public void start(Map<String, String> props) {
        readConfig(props);
        try {
            ehClient = EventHubClient.createFromConnectionStringSync(connection_string + ";TransportType=Amqp");
        } catch (Exception e) {
            log.error(e.getMessage());
        }
    }

    @Override
    public void put(Collection<SinkRecord> sinkRecords) {
        for (SinkRecord record : sinkRecords) {
            EventData sendEvent = new EventData((byte[]) record.value());
            try {
                ehClient.sendSync(sendEvent);
            }
            catch (Exception e) {
                log.error(e.getMessage());
            }
        }
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
        // do nothing
    }

    @Override
    public void stop() {
        ehClient.close();
    }
}
