package kafkaiot.sparkplug.topics;

public class DDATATopic extends SparkPlugBTopic {

    public DDATATopic(String deviceName) {
        super(deviceName);
    }

    @Override
    public String getTopic() {
        return BASE_TOPIC + deviceName + "/DDATA";
    }
}
