package kafkaiot.sparkplug;

import kafkaiot.sparkplug.topics.SparkPlugBTopic;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.tahu.message.SparkplugBPayloadEncoder;
import org.eclipse.tahu.message.model.SparkplugBPayload;
import org.eclipse.tahu.protobuf.SparkplugBProto;

import java.io.IOException;
import java.util.Date;

public class MessagePublisher {

    private MqttClient mqttClient;

    public MessagePublisher(MqttClient mqttClient) {
        this.mqttClient = mqttClient;
    }

    public void publishMessage(String topicName,
                               SparkplugBPayload payload) throws MqttException, IOException {
        MqttMessage message = new MqttMessage();
        payload.setTimestamp(new Date());
        SparkplugBPayloadEncoder encoder = new SparkplugBPayloadEncoder();
        message.setPayload(encoder.getBytes(payload, false));
        mqttClient.publish(topicName, message);
    }
}
