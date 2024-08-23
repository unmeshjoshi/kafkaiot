package kafkaiot.containers;

import org.jetbrains.annotations.NotNull;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.utility.DockerImageName;

public class RabitMQWithMqttContainer {
    private RabbitMQContainer rabbitMQContainer;

    public void start() {
        rabbitMQContainer = new RabbitMQContainer(DockerImageName.parse("rabbitmq:3"))
                .withExposedPorts(5672, 1883, 15672) // Expose AMQP, MQTT, and management ports
                .withPluginsEnabled("rabbitmq_mqtt");
        rabbitMQContainer.start();
    }

    public void stop() {
        rabbitMQContainer.stop();
    }

    public String getMqttListenAddress() {
        Integer mappedPort = rabbitMQContainer.getMappedPort(1883);
        String mqttListenAddress = "tcp://localhost:" + mappedPort;
        return mqttListenAddress;
    }
}
