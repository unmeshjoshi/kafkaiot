package kafkaiot;

import org.eclipse.paho.client.mqttv3.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.utility.DockerImageName;

public class MqttTest {
    private GenericContainer rabbitMQContainer;

    @Before
    public void startContainers() {
        rabbitMQContainer = new RabbitMQContainer(DockerImageName.parse("rabbitmq:3"))
                .withExposedPorts(5672, 1883, 15672) // Expose AMQP, MQTT, and management ports
                .withPluginsEnabled("rabbitmq_mqtt");
        ; // Expose MQTT port

        rabbitMQContainer.start();
    }

    @After
    public void stopContainers() {
        rabbitMQContainer.stop();
    }


    @Test
    public void produceAndConsumeMqtt() throws MqttException {
        Integer mappedPort = rabbitMQContainer.getMappedPort(1883);

        String host = "localhost";
        String broker = "tcp://" + host + ":" + mappedPort;

        System.out.println("broker = " + broker);

        String clientId = MqttAsyncClient.generateClientId();
        MqttClient client = new MqttClient(broker, clientId);
        MqttConnectOptions options = new MqttConnectOptions();
        client.connect(options);

        if (client.isConnected()) {
            client.setCallback(new MqttCallback() {
                public void messageArrived(String topic, MqttMessage message) throws Exception {
                    System.out.println("topic: " + topic);
                    System.out.println("qos: " + message.getQos());
                    System.out.println("message content: " + new String(message.getPayload()));
                }

                public void connectionLost(Throwable cause) {
                    System.out.println("connectionLost: " + cause.getMessage());
                }

                public void deliveryComplete(IMqttDeliveryToken token) {
                    System.out.println("deliveryComplete: " + token.isComplete());
                }
            });

            String topic = "spBv1.0/group1/#";
            int subQos = 1;

            client.subscribe(topic, subQos);

            SparkplugBProducer plc = new SparkplugBProducer(broker);
            plc.produceSparkPlugBMessage();

            client.disconnect();
            client.close();

        }
    }
}
