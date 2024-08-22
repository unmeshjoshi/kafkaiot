package kafkaiot.sparkplug.topics;

public class NBIRTHTopic extends SparkPlugBTopic {

    public NBIRTHTopic(String nodeName) {
        super(nodeName);
    }

    @Override
    public String getTopic() {
        return BASE_TOPIC + deviceName + "/NBIRTH";
    }
}
