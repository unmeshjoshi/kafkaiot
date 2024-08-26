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
        String edgeNodeName = "ChassisAssembly";
        SparkPlugEdgeNode edgeNode = createEdgeNodeInstance(edgeNodeName);

        SparkPlugBApplication primaryApplication =
                new SparkPlugBApplication(kafkaContainer.getSchemaRegistryUrl(),
                            rabbitMQContainer.getMqttListenAddress());

        primaryApplication.subcribeForSparkplugMessages(edgeNodeName);

        edgeNode.sendSparkplugMessages();

        //TODO: How to store schema Ids to be used for DData and NData messages?
        //TODO: How to handle schema updates?
        //TODO: How to handle errors when schema changes are not compatible?
        TestUtils.waitForCondition(() -> primaryApplication.hasRegisteredSchemaFor("ChassisAssembly")
                && primaryApplication.hasRegisteredSchemaFor("RobotArm"),
                10000, 2000,
                () -> "Waiting for Node and Device schemas to be registered");

    }


    @NotNull
    private SparkPlugEdgeNode createEdgeNodeInstance(String edgeNodeName) throws MqttException, SparkplugException {
        SparkPlugUDTTestDataBuilder testdataBuilder = new SparkPlugUDTTestDataBuilder();
        SparkPlugEdgeNode edgeNode = new SparkPlugEdgeNode(edgeNodeName,
                testdataBuilder.createChassisAssemblyUDT(), rabbitMQContainer.getMqttListenAddress());
        edgeNode.addDevice(new SparkPlugDevice("RobotArm",
                testdataBuilder.createRobotArmUDT()));
        return edgeNode;
    }

}
