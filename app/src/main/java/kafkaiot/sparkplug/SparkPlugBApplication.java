package kafkaiot.sparkplug;

import com.google.common.annotations.VisibleForTesting;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.tahu.message.SparkplugBPayloadDecoder;
import org.eclipse.tahu.message.model.SparkplugBPayload;

import java.io.IOException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

//TODO: Does this need to be a fully compliant SparkplugB Primary Application?
public class SparkPlugBApplication {
    private final MqttClient mqttClient;
    private final KafkaConnector kafkaConnector;

    public SparkPlugBApplication(String schemaRegistryUrl, String mqttListenAddress, String bootstrapServers) throws MqttException {
        this.kafkaConnector = new KafkaConnector(schemaRegistryUrl,
                bootstrapServers);
        mqttClient = MqttUtils.createMqttClient(mqttListenAddress);
    }

    public void subcribeForSparkplugMessages(String edgeNodeName) throws MqttException {
        mqttClient.setCallback(new SparkplugMessageCallback());
        subscribeToTopics(edgeNodeName);
    }

    private class SparkplugMessageCallback implements MqttCallback {
        @Override
        public void connectionLost(Throwable cause) {
            System.out.println("Connection lost: " + cause);
        }

        @Override
        public void messageArrived(String topic, MqttMessage message) {
            try {
                System.out.println("Message Arrived on topic " + topic);
                SparkplugBPayload inboundPayload = decodeSparkplugMessage(message);
                handleMessage(topic, inboundPayload);
            } catch (Exception e) {
                System.out.println("Error processing message: " + e.getMessage());
                throw new RuntimeException(e);
            }
        }

        @Override
        public void deliveryComplete(IMqttDeliveryToken token) {
            // Log the completion of message delivery if needed
        }
    }

    private void handleMessage(String topic, SparkplugBPayload payload) throws IOException, RestClientException {
        if (SparkPlugBTopic.NBIRTH.matchesTopicName(topic)) {
            handleNBirthMessage(topic, payload);
        } else if (SparkPlugBTopic.DBIRTH.matchesTopicName(topic)) {
            handleDBirthMessage(topic, payload);
        } else if (SparkPlugBTopic.DDATA.matchesTopicName(topic)) {
            handleDDataMessage(topic, payload);
        }
        // Add handlers for other message types as needed
    }

    private void handleDDataMessage(String topic, SparkplugBPayload payload) throws RestClientException, IOException {
        String deviceNodeName =
                SparkPlugBTopic.DDATA.getDeviceNameFromTopic(topic);
        kafkaConnector.produce(deviceNodeName, payload);
   }


    private void subscribeToTopics(String edgeNodeName) throws MqttException {
        mqttClient.subscribe(SparkPlugBTopic.NBIRTH.getNodeTopic(SparkPlugBTopic.WILDCARD));
        mqttClient.subscribe(SparkPlugBTopic.NDATA.getNodeTopic(SparkPlugBTopic.WILDCARD));
        mqttClient.subscribe(SparkPlugBTopic.DBIRTH.getDeviceTopic(edgeNodeName, SparkPlugBTopic.WILDCARD));
        mqttClient.subscribe(SparkPlugBTopic.DDATA.getDeviceTopic(edgeNodeName, SparkPlugBTopic.WILDCARD));
    }

    private void handleDBirthMessage(String topic, SparkplugBPayload inboundPayload) throws IOException, RestClientException {
        String deviseName = SparkPlugBTopic.DBIRTH.getDeviceNameFromTopic(topic);
        kafkaConnector.registerSchema(deviseName, inboundPayload);
    }

    private void handleNBirthMessage(String topic, SparkplugBPayload inboundPayload) throws IOException, RestClientException {
        String edgeNodeName =
                SparkPlugBTopic.NBIRTH.getEdgeNodeNameFromTopic(topic);
        kafkaConnector.registerSchema(edgeNodeName, inboundPayload);
    }

    private SparkplugBPayload decodeSparkplugMessage(MqttMessage message) throws Exception {
        SparkplugBPayloadDecoder decoder = new SparkplugBPayloadDecoder();
        SparkplugBPayload inboundPayload = decoder.buildFromByteArray(message.getPayload(), null);
        return inboundPayload;
    }

    public void createTopic(String topicName, int numPartitions,
                            int replicationFactor) {
        kafkaConnector.createTopic(topicName, numPartitions, (short) replicationFactor);
    }

    @VisibleForTesting
    public boolean hasRegisteredSchemaFor(String deviceName) throws RestClientException, IOException {
        return kafkaConnector.hasRegisteredSchemaFor(deviceName);
    }

    public List consumeKafkaMessages(String topic) {
        KafkaConsumer consumer = kafkaConnector.createConsumer();
        consumer.subscribe(Collections.singleton(topic));
        return pollRecords(consumer);
    }

    private static List<GenericRecord> pollRecords(KafkaConsumer consumer) {
        ConsumerRecords<String, GenericRecord> records =
                consumer.poll(Duration.of(1000,
                        ChronoUnit.MILLIS));
        List<GenericRecord> genericRecords = new ArrayList<>();
        for (ConsumerRecord<String, GenericRecord> record : records) {
            genericRecords.add(record.value());
        }
        return genericRecords;
    }


    public void close() {
        kafkaConnector.close();
    }
}
