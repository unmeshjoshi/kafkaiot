package kafkaiot.sparkplug;

import com.google.common.annotations.VisibleForTesting;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.avro.Schema;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.tahu.message.SparkplugBPayloadDecoder;
import org.eclipse.tahu.message.model.SparkplugBPayload;

import java.io.IOException;

public class SparkPlugBApplication {
    private final CachedSchemaRegistryClient schemaRegistryClient;
    private final AvroSchemaMapper avroSchemaMapper = new AvroSchemaMapper();
    private final MqttClient mqttClient;

    public SparkPlugBApplication(String schemaRegistryUrl, String mqttListenAddress) throws MqttException {
        schemaRegistryClient
                = new CachedSchemaRegistryClient(schemaRegistryUrl,
                100);
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
        }
        // Add handlers for other message types as needed
    }

    private void subscribeToTopics(String edgeNodeName) throws MqttException {
        mqttClient.subscribe(SparkPlugBTopic.NBIRTH.getNodeTopic(SparkPlugBTopic.WILDCARD));
        mqttClient.subscribe(SparkPlugBTopic.NDATA.getNodeTopic(SparkPlugBTopic.WILDCARD));
        mqttClient.subscribe(SparkPlugBTopic.DBIRTH.getDeviceTopic(edgeNodeName, SparkPlugBTopic.WILDCARD));
        mqttClient.subscribe(SparkPlugBTopic.DDATA.getDeviceTopic(edgeNodeName, SparkPlugBTopic.WILDCARD));
    }

    private void handleDBirthMessage(String topic, SparkplugBPayload inboundPayload) throws IOException, RestClientException {
        String deviceNodeName = SparkPlugBTopic.DBIRTH.getDeviceNameFromTopic(topic);
        Schema schema = avroSchemaMapper.generateAvroSchemaFromTemplate(inboundPayload.getMetrics(), deviceNodeName);
        schemaRegistryClient.register(deviceNodeName, schema);
    }

    private void handleNBirthMessage(String topic, SparkplugBPayload inboundPayload) throws IOException, RestClientException {
        String edgeNodeName =
                SparkPlugBTopic.NBIRTH.getEdgeNodeNameFromTopic(topic);
        Schema schema = avroSchemaMapper.generateAvroSchemaFromTemplate(inboundPayload.getMetrics(), edgeNodeName);
        schemaRegistryClient.register(edgeNodeName, schema);
    }

    private static SparkplugBPayload decodeSparkplugMessage(MqttMessage message) throws Exception {
        SparkplugBPayloadDecoder decoder = new SparkplugBPayloadDecoder();
        SparkplugBPayload inboundPayload = decoder.buildFromByteArray(message.getPayload(), null);
        return inboundPayload;
    }

    @VisibleForTesting
    public boolean hasRegisteredSchemaFor(String deviceName) throws RestClientException, IOException {
        return schemaRegistryClient.getAllVersions(deviceName)
                .size() > 0;
    }
}
