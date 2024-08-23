package kafkaiot.sparkplug;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import kafkaiot.containers.ConfluentKafkaContainer;
import kafkaiot.containers.RabitMQWithMqttContainer;
import org.apache.avro.Schema;
import org.apache.kafka.test.TestUtils;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.eclipse.tahu.SparkplugException;
import org.eclipse.tahu.message.SparkplugBPayloadDecoder;
import org.eclipse.tahu.message.model.Metric;
import org.eclipse.tahu.message.model.SparkplugBPayload;
import org.eclipse.tahu.message.model.Template;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class SchemaRegistryIntegrationTest {
    private ConfluentKafkaContainer kafkaContainer;
    private RabitMQWithMqttContainer rabbitMQContainer;
    private SchemaRegistryClient schemaRegistryClient;

    @Before
    public void startContainers() {
        kafkaContainer = new ConfluentKafkaContainer();
        kafkaContainer.start();

        rabbitMQContainer = new RabitMQWithMqttContainer();
        rabbitMQContainer.start();

        schemaRegistryClient
                = new CachedSchemaRegistryClient(kafkaContainer.getSchemaRegistryUrl(),
                100);

    }

    @After
    public void stopContainers() {
        kafkaContainer.stop();
        rabbitMQContainer.stop();
    }

    @Test
    public void registersSchemasFromDBirthMessagesToSchemaRegistry() throws SparkplugException, MqttException, InterruptedException {
        SparkPlugUDTTestDataBuilder testdataBuilder = new SparkPlugUDTTestDataBuilder();
        // Create UDTs for each device
        Template chassisAssemblyUDTTemplate = testdataBuilder.createChassisAssemblyUDT();
        Template robotArmUDTTemplate = testdataBuilder.createRobotArmUDT();

        String clientId = MqttAsyncClient.generateClientId();
        MemoryPersistence persistence = new MemoryPersistence();
        MqttClient mqttClient =
                new MqttClient(rabbitMQContainer.getMqttListenAddress(), clientId, persistence);
        mqttClient.connect();

        List receivedMessages = new ArrayList<>();

        String edgeNodeName = "ChassisAssembly";
        subcribeForSparkplugMessages(mqttClient, edgeNodeName, receivedMessages);
        // Create SparkPlugDevice instances
        SparkPlugEdgeNode edgeNode = new SparkPlugEdgeNode(edgeNodeName,
                chassisAssemblyUDTTemplate, mqttClient);


        edgeNode.addDevice(new SparkPlugDevice("RobotArm",
                robotArmUDTTemplate));


        edgeNode.sendNBIRTH();
        edgeNode.sendNDATA();
        edgeNode.sendDeviceMessages();

        //TODO: How to store schema Ids to be used for DData and NData messages?
        //TODO: How to handle schema updates?
        //TODO: How to handle errors when schema changes are not compatible?
        TestUtils.waitForCondition(() -> schemaRegistryClient.getAllVersions(
                "ChassisAssembly").size() > 0 && schemaRegistryClient.getAllVersions(
                        "RobotArm").size() > 0,
                            10000, 2000,
                () -> "Waiting for Node and Device schemas to be registered");

    }

    private void subcribeForSparkplugMessages(MqttClient mqttClient, String edgeNodeName, List receivedMessages) throws MqttException {
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

                    SparkplugBPayloadDecoder decoder = new SparkplugBPayloadDecoder();
                    SparkplugBPayload inboundPayload = decoder.buildFromByteArray(message.getPayload(), null);

                    AvroSchemaMapper avroSchemaMapper = new AvroSchemaMapper();

                    if (isNBirth(topic)) {
                        String edgeNodeName =
                                SparkPlugBTopic.NBIRTH.getEdgeNodeNameFromTopic(topic);
                        Schema schema = avroSchemaMapper.generateAvroSchemaFromTemplate(inboundPayload.getMetrics(), edgeNodeName);
                        schemaRegistryClient.register(edgeNodeName, schema);

                    } else if (isDBirth(topic)) {
                        String deviceNodeName = SparkPlugBTopic.DBIRTH.getDeviceNameFromTopic(topic);
                        Schema schema = avroSchemaMapper.generateAvroSchemaFromTemplate(inboundPayload.getMetrics(), deviceNodeName);
                        schemaRegistryClient.register(deviceNodeName, schema);
                    }
                    // Debug
                    for (Metric metric : inboundPayload.getMetrics()) {
                        System.out.println("Metric " + metric.getName() + "=" + metric.getValue());
                    }

                    receivedMessages.add(inboundPayload);

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
}
