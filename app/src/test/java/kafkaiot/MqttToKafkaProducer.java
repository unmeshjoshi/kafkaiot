package kafkaiot;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import java.util.Properties;

public class MqttToKafkaProducer {


    private static final String SCHEMA_REGISTRY_URL = "http://localhost:8081";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static KafkaProducer<String, String> producer;
    private static String broker;

    public MqttToKafkaProducer(String broker) {
        this.broker = broker;
    }

    public void consumeAndProduce() {
        String clientId = MqttAsyncClient.generateClientId();
        MemoryPersistence persistence = new MemoryPersistence();

        // Kafka producer configuration
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put("schema.registry.url", SCHEMA_REGISTRY_URL);

        producer = new KafkaProducer<>(props);

        try {
            MqttClient mqttClient = new MqttClient(broker, clientId, persistence);
            mqttClient.setCallback(new MqttCallback() {
                @Override
                public void connectionLost(Throwable cause) {
                    // Log the loss of connection
                    System.out.println("Connection lost = " + cause);
                }

                @Override
                public void messageArrived(String topic, MqttMessage message) {
                    String payload = new String(message.getPayload());
                    String key = extractKeyFromTopic(topic);

                    System.out.println("received   " + key + " = " + payload);
                    // Produce the message to Kafka
                    ProducerRecord<String, String> record = new ProducerRecord<>("sparkplug_b_messages", key, payload);
                    producer.send(record);
                }

                @Override
                public void deliveryComplete(IMqttDeliveryToken token) {
                    // Log the completion of message delivery
                }
            });
            mqttClient.connect();
            mqttClient.subscribe("spBv1.0/group1/#");

        } catch (MqttException me) {
            me.printStackTrace();
        }
    }

    private static String extractKeyFromTopic(String topic) {
        // Assuming topic format: spBv1.0/group1/<message_type>/node1/device1
        String[] parts = topic.split("/");
        return parts[3] + "_" + parts[4];
    }
}
