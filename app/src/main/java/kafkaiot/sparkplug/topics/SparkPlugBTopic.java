package kafkaiot.sparkplug.topics;

public abstract class SparkPlugBTopic {
    protected static final String BASE_TOPIC = "sparkplug/b/";
    protected final String deviceName;

    public SparkPlugBTopic(String deviceName) {
        this.deviceName = deviceName;
    }

    public abstract String getTopic();
}
