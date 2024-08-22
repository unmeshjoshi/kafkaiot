package kafkaiot.sparkplug;

import kafkaiot.SchemaRegistryContainer;
import org.eclipse.tahu.SparkplugException;
import org.eclipse.tahu.message.model.Template;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.utility.DockerImageName;

public class SchemaRegistryIntegrationTest {
    private SchemaRegistryContainer schemaRegistryContainer;
    private RabbitMQContainer rabbitMQContainer;

    @Before
    public void startContainers() {
        schemaRegistryContainer = new SchemaRegistryContainer();
        schemaRegistryContainer.start();
        rabbitMQContainer = new RabbitMQContainer(DockerImageName.parse("rabbitmq:3"))
                .withExposedPorts(5672, 1883, 15672) // Expose AMQP, MQTT, and management ports
                .withPluginsEnabled("rabbitmq_mqtt");
        rabbitMQContainer.start();
    }

    @After
    public void stopContainers() {
        schemaRegistryContainer.stop();
        rabbitMQContainer.stop();
    }
    @Test
    public void registersSchemasFromDBirthMessagesToSchemaRegistry() throws SparkplugException {
        UDTExample example = new UDTExample();

        // Create UDTs for each device
        Template chassisAssemblyTemplate = example.createChassisAssemblyUDT();
        Template robotArmTemplate = example.createRobotArmUDT();

        // Create SparkPlugDevice instances
        SparkPlugEdgeNode chassisAssemblyDevice = new SparkPlugEdgeNode("ChassisAssembly",
                chassisAssemblyTemplate);
        chassisAssemblyDevice.addDevice(new SparkPlugDevice("RobotArm",
                robotArmTemplate));

    }
}
