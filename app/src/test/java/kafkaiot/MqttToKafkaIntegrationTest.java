package kafkaiot;

import com.github.dockerjava.api.command.InspectContainerResponse;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class MqttToKafkaIntegrationTest {

    private static KafkaContainer kafkaContainer;
    private static RabbitMQContainer rabbitMQContainer;

    @BeforeAll
    public static void startContainers() {
        kafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.3.1"));
        kafkaContainer.start();
        rabbitMQContainer = new RabbitMQContainer(DockerImageName.parse("rabbitmq:3"))
                .withExposedPorts(5672, 1883, 15672) // Expose AMQP, MQTT, and management ports
                .withPluginsEnabled("rabbitmq_mqtt");
        rabbitMQContainer.start();
    }

    @AfterAll
    public static void stopContainers() {
        kafkaContainer.stop();
        rabbitMQContainer.stop();
    }

    @Test
    public void factoryChasisAssemblyMonitoring() throws Exception {
        String binds = rabbitMQContainer.getHost();
        InspectContainerResponse containerInfo = rabbitMQContainer.getContainerInfo();

        // Simulate PLC

        Integer mappedPort = rabbitMQContainer.getMappedPort(1883);
        System.out.println("mappedPort = " + mappedPort);

        String mqttListenAddress = "tcp://localhost:" + mappedPort;

        new Scratchpad(mqttListenAddress).produceSparkPlugBMessage();
        // Start MQTT to Kafka producer
        new Thread(() -> new MqttToKafkaProducer(mqttListenAddress).consumeAndProduce()).start();

        // Kafka consumer configuration
        Map<String, Object> consumerProps =
                KafkaTestUtils.consumerProps(kafkaContainer.getBootstrapServers(), "testGroup", "true");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Collections.singletonList("sparkplug_b_messages"));

        ConsumerRecord<String, String> received = KafkaTestUtils.getSingleRecord(consumer, "sparkplug_b_messages");
        assertEquals("node1_device1", received.key());
        assertEquals("{\"namespace\":\"spBv1.0\",\"group_id\":\"group1\",\"message_type\":\"DBIRTH\",\"edge_node_id\":\"node1\",\"device_id\":\"device1\",\"timestamp\":1627861234567,\"metrics\":[{\"name\":\"Temperature\",\"type\":\"double\",\"value\":23.5},{\"name\":\"Status\",\"type\":\"string\",\"value\":\"OK\"}],\"udts\":[]}", received.value());
    }
}
