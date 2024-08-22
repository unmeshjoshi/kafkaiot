package kafkaiot;

import org.eclipse.paho.client.mqttv3.*;

import java.util.Random;

public class SparkplugBProducer {
    String broker;

    public SparkplugBProducer(String broker) {
        this.broker = broker;
    }

    public String getStatus() {
        return "OK";
    }

    public long getTemperature() {
        return new Random().nextLong(100);
    }


    public void produceSparkPlugBMessage() {
        String clientId = MqttAsyncClient.generateClientId();

        try {
            MqttClient sampleClient = new MqttClient(broker, clientId);

            MqttConnectOptions connOpts = new MqttConnectOptions();
            connOpts.setCleanSession(true);
            System.out.println("Connecting to broker: "+broker);
            sampleClient.connect(connOpts);
            
            // Simulate sending a DBIRTH message
            String topic = "spBv1.0/group1/DBIRTH/node1/device1";
            String payload = "{\"namespace\":\"spBv1.0\",\"group_id\":\"group1\",\"message_type\":\"DBIRTH\",\"edge_node_id\":\"node1\",\"device_id\":\"device1\",\"timestamp\":1627861234567,\"metrics\":[{\"name\":\"Temperature\",\"type\":\"double\",\"value\":23.5},{\"name\":\"Status\",\"type\":\"string\",\"value\":\"OK\"}],\"udts\":[]}";
            MqttMessage message = new MqttMessage(payload.getBytes());
            message.setQos(2);
            sampleClient.publish(topic, message);

            // Simulate sending a DDATA message
            topic = "spBv1.0/group1/DDATA/node1/device1";
            payload = "{\"Temperature\":24.1,\"Status\":\"OK\"}";
            message = new MqttMessage(payload.getBytes());
            message.setQos(2);
            sampleClient.publish(topic, message);

            sampleClient.disconnect();
            System.out.println("Messages published");
        } catch (MqttException me) {
            me.printStackTrace();
        }
    }
}
