package kafkaiot.sparkplug;

import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaFuture;
import org.eclipse.tahu.message.model.Metric;
import org.eclipse.tahu.message.model.SparkplugBPayload;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;
import java.util.Set;

class KafkaConnector {
    private final Producer<String, GenericRecord> producer;
    private final AdminClient adminClient;
    private final NodeSchemaRegistry nodeSchemaRegistry;

    public KafkaConnector(String schemaRegistryUrl, String bootstrapServers) {
        this.nodeSchemaRegistry = new NodeSchemaRegistry(schemaRegistryUrl);

        Properties props = kafkaProperties(bootstrapServers, schemaRegistryUrl);
        this.producer = new KafkaProducer<>(props);
        this.adminClient = AdminClient.create(props);
    }

    public boolean topicExists(String topicName) {
        try {
            ListTopicsResult listTopicsResult = adminClient.listTopics();
            KafkaFuture<Set<String>> names = listTopicsResult.names();
            Set<String> topics = names.get();
            return topics.contains(topicName);
        } catch (Exception e) {
            System.err.println("Error checking topic existence: " + e.getMessage());
            return false;
        }
    }

    private Properties kafkaProperties(String bootstrapServers, String schemaRegistryUrl) {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
        props.put("schema.registry.url", schemaRegistryUrl);
        return props;
    }

    public void produce(String deviceNodeName, SparkplugBPayload payload) throws RestClientException, IOException {

        if (topicExists(deviceNodeName)) {
            SchemaMetadata schemaMetadata =
                    getLatestSchemaMetadata(deviceNodeName);
            Schema schema = new Schema.Parser().parse(schemaMetadata.getSchema());
            GenericRecord record = convertPayloadToGenericRecord(payload, schema);
            produce(deviceNodeName, record);

        } else {
            System.out.println("Topic " + deviceNodeName + " does not exist. Message not sent.");
        }

    }

    private void produce(String deviceNodeName, GenericRecord record) {
        //What is the topic name
        ProducerRecord<String, GenericRecord> producerRecord =
                new ProducerRecord<>(deviceNodeName, deviceNodeName, record);
        producer.send(producerRecord);
    }


    private GenericRecord convertPayloadToGenericRecord(SparkplugBPayload payload, Schema schema) {
        GenericRecord record = new GenericData.Record(schema);
        for (Metric metric : payload.getMetrics()) {
            record.put(metric.getName(), metric.getValue());
        }
        return record;
    }

    public SchemaMetadata getLatestSchemaMetadata(String deviceNodeName) throws RestClientException, IOException {
        return nodeSchemaRegistry.getLatestSchemaMetadata(deviceNodeName);
    }

    public void registerSchema(String nodeName, SparkplugBPayload inboundPayload) throws RestClientException, IOException {
        nodeSchemaRegistry.registerSchema(nodeName, inboundPayload);
    }

    public boolean hasRegisteredSchemaFor(String deviceName) throws RestClientException, IOException {
        return nodeSchemaRegistry.hasRegisteredSchemaFor(deviceName);
    }

    public void createTopic(String topicName, int numPartitions,
                            short replicationFactor) {
        NewTopic newTopic = new NewTopic(topicName, numPartitions, replicationFactor);
        adminClient.createTopics(Arrays.asList(newTopic));
    }

    public void close() {
        adminClient.close();
        producer.close();
    }
}
