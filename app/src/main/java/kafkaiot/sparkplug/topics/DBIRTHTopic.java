package kafkaiot.sparkplug.topics;

public class DBIRTHTopic extends SparkPlugBTopic {

    public DBIRTHTopic(String deviceName) {
        super(deviceName);
    }

    @Override
    public String getTopic() {
        return BASE_TOPIC + deviceName + "/DBIRTH";
    }
}
