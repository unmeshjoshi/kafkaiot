package kafkaiot;

import com.github.dockerjava.api.command.InspectContainerResponse;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class MqttToKafkaIntegrationTest {

    private static KafkaContainer kafkaContainer;
    private static RabbitMQContainer rabbitMQContainer;

    @Before
    public void startContainers() {
        kafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.3.9"));
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
    public void factoryChasisAssemblyMonitoring() throws Exception {
        String binds = rabbitMQContainer.getHost();
        InspectContainerResponse containerInfo = rabbitMQContainer.getContainerInfo();

        // Simulate PLC

        Integer mappedPort = rabbitMQContainer.getMappedPort(1883);
        System.out.println("mappedPort = " + mappedPort);

        String mqttListenAddress = "tcp://localhost:" + mappedPort;
        // Start MQTT to Kafka producer
        String bootstrapServers = kafkaContainer.getBootstrapServers();
        new MqttToKafkaProducer(mqttListenAddress,
                bootstrapServers).consumeMqttAndProduceOnKafka();

        new SparkplugBProducer(mqttListenAddress).produceSparkPlugBMessage();


        // Kafka consumer configuration
        Map<String, Object> consumerProps =
                KafkaTestUtils.consumerProps(kafkaContainer.getBootstrapServers(), "testGroup", "true");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Collections.singletonList("sparkplug_b_messages"));

        ConsumerRecords<String, String> received = KafkaTestUtils.getRecords(consumer);
        Iterable<ConsumerRecord<String, String>> sparkplugBMessages = received.records("sparkplug_b_messages");

        Iterator<ConsumerRecord<String, String>> iterator = sparkplugBMessages.iterator();

        ConsumerRecord<String, String> dbirthMessage = iterator.next();
        assertEquals("node1_device1", dbirthMessage.key());
        assertEquals("{\"namespace\":\"spBv1.0\",\"group_id\":\"group1\",\"message_type\":\"DBIRTH\",\"edge_node_id\":\"node1\",\"device_id\":\"device1\",\"timestamp\":1627861234567,\"metrics\":[{\"name\":\"Temperature\",\"type\":\"double\",\"value\":23.5},{\"name\":\"Status\",\"type\":\"string\",\"value\":\"OK\"}],\"udts\":[]}", dbirthMessage.value());

        ConsumerRecord<String, String> ddataMessage = iterator.next();
        assertEquals("node1_device1", ddataMessage.key());

    }
}
