package kafkaiot.sparkplug;

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
        mqttClient.setCallback(new MqttCallback() {
            @Override
            public void connectionLost(Throwable cause) {
                // Log the loss of connection
                System.out.println("Connection lost = " + cause);
            }

            @Override //TODO: We should have separate subscribers for NBIRTH
            // and DBIRTH, so that schema creation can be separated from this
            // method.
            public void messageArrived(String topic, MqttMessage message) {
                try {
                    System.out.println("Message Arrived on topic " + topic);
                    SparkplugBPayload inboundPayload = decodeSparkplugMessage(message);
                    if (isNBirth(topic)) {
                        handleNBirthMessage(topic, inboundPayload);

                    } else if (isDBirth(topic)) {
                        handleDBirthMessage(topic, inboundPayload);
                    }

                } catch (Exception e) {
                    System.out.println("e = " + e.getMessage());
                    throw new RuntimeException(e);
                }

            }

            private boolean isDBirth(String topic) {
                return topic.contains(SparkPlugBTopic.DBIRTH.name());
            }

            private boolean isNBirth(String topic) {
                return topic.contains(SparkPlugBTopic.NBIRTH.name());
            }

            @Override
            public void deliveryComplete(IMqttDeliveryToken token) {
                // Log the completion of message delivery
            }
        });

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

    public boolean hasRegisteredSchemaFor(String deviceName) throws RestClientException, IOException {
        return schemaRegistryClient.getAllVersions(deviceName)
                .size() > 0;
    }
}
