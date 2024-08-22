package kafkaiot.sparkplug;

import com.google.protobuf.InvalidProtocolBufferException;
import org.eclipse.tahu.protobuf.SparkplugBProto;
import org.eclipse.tahu.protobuf.SparkplugBProto.Payload.Metric;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class SparkPlugDeviceTest {

    //A test directly using protobuff. This could be useful if we use out own
    // proto file of sparkplugb specs from https://github.com/eclipse/tahu/tree/master/sparkplug_b
    //But we will use eclipse tahu library directly everywhere else.
    @Test
    public void buildsDBirthMessagePayloadWithTemplate() throws InvalidProtocolBufferException {
        SparkplugBProto.Payload.Template template =
                SparkplugBProto.Payload.Template.newBuilder()
                .addMetrics(Metric.newBuilder().setName("temperature").setDatatype(SparkplugBProto.DataType.Double.getNumber()).setDoubleValue(22.5).build())
                .addMetrics(Metric.newBuilder().setName("pressure").setDatatype(SparkplugBProto.DataType.Double.getNumber()).setDoubleValue(101.3).build())
                .build();

        ;
        List<Metric> metrics = Arrays.asList(
                Metric.newBuilder().setDatatype(SparkplugBProto.DataType.Template.getNumber()).setTemplateValue(template).build()
        );

        SparkPlugDevice device = new SparkPlugDevice("RobotArm01", metrics);
        SparkplugBProto.Payload publishedPayload = device.buildDbirthMessage();


        byte[] byteArray = publishedPayload.toByteArray();
        SparkplugBProto.Payload receivedPayload = SparkplugBProto.Payload.parseFrom(byteArray);

        assertEquals(publishedPayload, receivedPayload);
    }

}