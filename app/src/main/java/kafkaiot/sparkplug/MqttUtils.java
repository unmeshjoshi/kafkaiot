package kafkaiot.sparkplug;

import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

public class MqttUtils {
    public static MqttClient createMqttClient(String mqttListenAddress) throws MqttException {
        final MqttClient mqttClient;
        String clientId = MqttAsyncClient.generateClientId();
        MemoryPersistence persistence = new MemoryPersistence();
        mqttClient = new MqttClient(mqttListenAddress, clientId, persistence);
        mqttClient.connect();
        return mqttClient;
    }
}
