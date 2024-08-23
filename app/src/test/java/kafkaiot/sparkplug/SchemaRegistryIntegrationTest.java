package kafkaiot.sparkplug;

import kafkaiot.containers.ConfluentKafkaContainer;
import org.eclipse.tahu.SparkplugException;
import org.eclipse.tahu.message.model.Template;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.utility.DockerImageName;

public class SchemaRegistryIntegrationTest {
    private ConfluentKafkaContainer kafkaContainer;
    private RabbitMQContainer rabbitMQContainer;

    @Before
    public void startContainers() {
        kafkaContainer = new ConfluentKafkaContainer();
        kafkaContainer.start();

        rabbitMQContainer = new RabbitMQContainer(DockerImageName.parse("rabbitmq:3"))
                .withExposedPorts(5672, 1883, 15672) // Expose AMQP, MQTT, and management ports
                .withPluginsEnabled("rabbitmq_mqtt");
        rabbitMQContainer.start();
    }

    @After
    public void stopContainers() {
        kafkaContainer.stop();
        rabbitMQContainer.stop();
    }
    @Test
    public void registersSchemasFromDBirthMessagesToSchemaRegistry() throws SparkplugException {
        SparkPlugUDTTestDataBuilder testdataBuilder = new SparkPlugUDTTestDataBuilder();
        // Create UDTs for each device
        Template chassisAssemblyUDTTemplate = testdataBuilder.createChassisAssemblyUDT();
        Template robotArmUDTTemplate = testdataBuilder.createRobotArmUDT();

        // Create SparkPlugDevice instances
        SparkPlugEdgeNode chassisAssemblyDevice = new SparkPlugEdgeNode("ChassisAssembly",
                chassisAssemblyUDTTemplate);
        chassisAssemblyDevice.addDevice(new SparkPlugDevice("RobotArm",
                robotArmUDTTemplate));




    }
}
