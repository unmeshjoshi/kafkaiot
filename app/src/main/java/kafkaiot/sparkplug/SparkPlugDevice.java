package kafkaiot.sparkplug;

import org.eclipse.tahu.protobuf.SparkplugBProto.Payload;
import org.eclipse.tahu.protobuf.SparkplugBProto.Payload.Metric;

import java.util.List;

public class SparkPlugDevice {

    private final String deviceName;

    private final List<Metric> deviceMetrics;

    public SparkPlugDevice(String deviceName, List<Metric> deviceMetrics) {
        this.deviceName = deviceName;
        this.deviceMetrics = deviceMetrics;
    }

    public Payload buildDbirthMessage() {
        Payload.Builder payloadBuilder = Payload.newBuilder();

        // Add metrics to the payload
        for (Metric metric : deviceMetrics) {
            payloadBuilder.addMetrics(metric);
        }

        // Set the timestamp and sequence number
        payloadBuilder.setTimestamp(System.currentTimeMillis());
        payloadBuilder.setSeq(0);  // Sequence can be incremented as needed

        return payloadBuilder.build();
   }
}
