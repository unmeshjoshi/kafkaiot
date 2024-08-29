package kafkaiot.sparkplug;

import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.eclipse.tahu.message.model.Metric;
import org.eclipse.tahu.message.model.SparkplugBPayload;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.Future;

class KafkaConnector {
    private final Producer<String, GenericRecord> producer;
    private final AdminClient adminClient;
    private final NodeSchemaRegistry nodeSchemaRegistry;
    private final String bootstrapServers;
    private final String schemaRegistryUrl;

    public KafkaConnector(String schemaRegistryUrl, String bootstrapServers) {
        this.schemaRegistryUrl = schemaRegistryUrl;
        this.nodeSchemaRegistry = new NodeSchemaRegistry(schemaRegistryUrl);
        this.bootstrapServers = bootstrapServers;

        Map kafkaProperties = producerProps(bootstrapServers, schemaRegistryUrl);
        this.producer = new KafkaProducer<>(kafkaProperties);

        this.adminClient = AdminClient.create(kafkaProperties);
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


    private Map<String, Object> consumerProps(String bootstrapServers,
                                              String schemaRegistryUrl,
                                              String group) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                KafkaAvroDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }

    private Map<String, Object> producerProps(String bootstrapServers,
                                              String schemaRegistryUrl) {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                schemaRegistryUrl);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                KafkaAvroSerializer.class);
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
        Future<RecordMetadata> send = producer.send(producerRecord);
        try {
            ;
            System.out.println("Producing data on  = " + deviceNodeName +
                    "topic. offset=" + send.get().offset());

        } catch (Exception e) {
            e.printStackTrace();
        }

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

    public KafkaConsumer createConsumer() {
        return
                new KafkaConsumer<>(consumerProps(bootstrapServers,
                        schemaRegistryUrl, "SparkPlugBKafka-Group"));

    }
}
