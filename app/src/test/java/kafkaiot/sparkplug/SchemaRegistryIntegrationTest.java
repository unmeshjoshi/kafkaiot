package kafkaiot.sparkplug;

import kafkaiot.containers.ConfluentKafkaContainer;
import kafkaiot.containers.RabitMQWithMqttContainer;
import org.apache.kafka.test.TestUtils;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.tahu.SparkplugException;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

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
        // Create UDTs for each device
        SparkPlugEdgeNode edgeNode = createEdgeNodeInstance();


        SparkPlugBApplication primaryApplication =
                new SparkPlugBApplication(kafkaContainer.getSchemaRegistryUrl(),
                            rabbitMQContainer.getMqttListenAddress(),
                        kafkaContainer.getBootstrapServers());

        createKafkaTopics(primaryApplication, edgeNode);

        primaryApplication.subcribeForSparkplugMessages(edgeNode.getEdgeNodeName());

        edgeNode.sendSparkplugMessages();

        //TODO: How to handle schema updates?
        //TODO: How to handle errors when schema changes are not compatible?
        TestUtils.waitForCondition(() -> primaryApplication.hasRegisteredSchemaFor(edgeNode.getEdgeNodeName() + "-value")
                && primaryApplication.hasRegisteredSchemaFor(edgeNode.getDevices().get(0).getDeviceName() + "-value"),
                10000, 2000,
                () -> "Waiting for Node and Device schemas to be registered");

    }

    private static void createKafkaTopics(SparkPlugBApplication primaryApplication, SparkPlugEdgeNode edgeNode) {
        primaryApplication.createTopic(edgeNode.getEdgeNodeName(), 3, 1);
        List<SparkPlugDevice> devices = edgeNode.getDevices();
        for (SparkPlugDevice device : devices) {
            primaryApplication.createTopic(device.getDeviceName(), 3, 1);
        }
    }


    private SparkPlugEdgeNode createEdgeNodeInstance() throws MqttException, SparkplugException {
        String edgeNodeName = "ChassisAssembly";
        SparkPlugUDTTestDataBuilder testdataBuilder = new SparkPlugUDTTestDataBuilder();
        SparkPlugEdgeNode edgeNode = new SparkPlugEdgeNode(edgeNodeName,
                testdataBuilder.createChassisAssemblyUDT(), rabbitMQContainer.getMqttListenAddress());
        edgeNode.addDevice(new SparkPlugDevice("RobotArm",
                testdataBuilder.createRobotArmUDT()));
        return edgeNode;
    }

}
