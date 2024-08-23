package kafkaiot.sparkplug;

import kafkaiot.containers.ConfluentKafkaContainer;
import kafkaiot.containers.RabitMQWithMqttContainer;
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

    @Before
    public void startContainers() {
        kafkaContainer = new ConfluentKafkaContainer();
        kafkaContainer.start();

        rabbitMQContainer = new RabitMQWithMqttContainer();
        rabbitMQContainer.start();
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

        TestUtils.waitForCondition(() -> receivedMessages.size() > 2, "waiting for messages to arrive");

    }

    private static void subcribeForSparkplugMessages(MqttClient mqttClient, String edgeNodeName, List receivedMessages) throws MqttException {
        mqttClient.setCallback(new MqttCallback() {
            @Override
            public void connectionLost(Throwable cause) {
                // Log the loss of connection
                System.out.println("Connection lost = " + cause);
            }

            @Override
            public void messageArrived(String topic, MqttMessage message) {
                try {

                    System.out.println("Message Arrived on topic " + topic);

                    SparkplugBPayloadDecoder decoder = new SparkplugBPayloadDecoder();
                    SparkplugBPayload inboundPayload = decoder.buildFromByteArray(message.getPayload(), null);

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
