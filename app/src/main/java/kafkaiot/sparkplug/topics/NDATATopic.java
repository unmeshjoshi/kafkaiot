package kafkaiot.sparkplug.topics;

public class NDATATopic extends SparkPlugBTopic {

    public NDATATopic(String nodeName) {
        super(nodeName);
    }

    @Override
    public String getTopic() {
        return BASE_TOPIC + deviceName + "/NDATA";
    }
}
