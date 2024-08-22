package kafkaiot.sparkplug.topics;

import kafkaiot.sparkplug.SparkPlugBTopic;
import org.junit.Test;
import static org.junit.Assert.*;

public class SparkPlugBTopicTest {

    @Test
    public void testGetDeviceTopic() {
        SparkPlugBTopic topicType = SparkPlugBTopic.DDATA;
        String edgeNode = "EdgeNode1";
        String deviceName = "Device1";

        String expectedTopic = "spBv1.0/sparkplug/b/DDATA/EdgeNode1/Device1";
        String actualTopic = topicType.getDeviceTopic(edgeNode, deviceName);

        assertEquals("The device topic should be correctly formatted.", expectedTopic, actualTopic);
    }

    @Test
    public void testGetNodeTopic() {
        SparkPlugBTopic topicType = SparkPlugBTopic.NBIRTH;
        String edgeNode = "EdgeNode1";

        String expectedTopic = "spBv1.0/sparkplug/b/NBIRTH/EdgeNode1";
        String actualTopic = topicType.getNodeTopic(edgeNode);

        assertEquals("The node topic should be correctly formatted.", expectedTopic, actualTopic);
    }

    @Test
    public void testGetTopic() {
        SparkPlugBTopic topicType = SparkPlugBTopic.STATE;

        String expectedTopic = "spBv1.0/sparkplug/b/STATE/applicationName";
        String actualTopic = topicType.getNodeTopic("applicationName");

        assertEquals("The topic should be correctly formatted.", expectedTopic, actualTopic);
    }

    @Test
    public void testGetDeviceTopicWithWildcard() {
        SparkPlugBTopic topicType = SparkPlugBTopic.DDATA;
        String edgeNode = "EdgeNode1";
        String deviceName = SparkPlugBTopic.WILDCARD;

        String expectedTopic = "spBv1.0/sparkplug/b/DDATA/EdgeNode1/#";
        String actualTopic = topicType.getDeviceTopic(edgeNode, deviceName);

        assertEquals("The device topic with wildcard should be correctly formatted.", expectedTopic, actualTopic);
    }

    @Test
    public void testGetNodeTopicWithWildcard() {
        SparkPlugBTopic topicType = SparkPlugBTopic.NDATA;
        String edgeNode = SparkPlugBTopic.WILDCARD;

        String expectedTopic = "spBv1.0/sparkplug/b/NDATA/#";
        String actualTopic = topicType.getNodeTopic(edgeNode);

        assertEquals("The node topic with wildcard should be correctly formatted.", expectedTopic, actualTopic);
    }


    @Test
    public void testNullParametersInGetDeviceTopic() {
        SparkPlugBTopic topicType = SparkPlugBTopic.DDATA;

        try {
            topicType.getDeviceTopic(null, "Device1");
            fail("Expected AssertionError for null edge node name");
        } catch (AssertionError e) {
            // Expected
        }

        try {
            topicType.getDeviceTopic("EdgeNode1", null);
            fail("Expected AssertionError for null device name");
        } catch (AssertionError e) {
            // Expected
        }
    }

    @Test
    public void testNullParametersInGetNodeTopic() {
        SparkPlugBTopic topicType = SparkPlugBTopic.NBIRTH;

        try {
            topicType.getNodeTopic(null);
            fail("Expected AssertionError for null edge node name");
        } catch (AssertionError e) {
            // Expected
        }
    }
}
