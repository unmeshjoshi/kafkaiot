package kafkaiot.sparkplug.topics;

public enum SparkPlugBTopic {
    DBIRTH("DBIRTH"),
    DDATA("DDATA"),
    NBIRTH("NBIRTH"),
    NDATA("NDATA"),
    STATE("STATE"),
    NCMD("NCMD"),
    DCMD("DCMD"),
    ALERT("ALERT");

    public static final String WILDCARD = "#"; // Public constant for wildcard

    private static final String NAMESPACE = "spBv1.0";
    private static final String GROUP_ID = "sparkplug/b";

    private final String topicType;

    SparkPlugBTopic(String topicType) {
        this.topicType = topicType;
    }

    /**
     * Constructs a topic string for device messages with edge node and device name.
     *
     * @param edgeNode   The edge node name (must not be null).
     * @param deviceName The device name (must not be null).
     * @return The constructed topic string.
     */
    public String getDeviceTopic(String edgeNode, String deviceName) {
        assert edgeNode != null : "Edge node name must not be null";
        assert deviceName != null : "Device name must not be null";
        return String.format("%s/%s/%s/%s/%s", NAMESPACE, GROUP_ID, topicType, edgeNode, deviceName);
    }

    /**
     * Constructs a topic string for node messages with edge node only.
     *
     * @param edgeNode The edge node name (must not be null).
     * @return The constructed topic string.
     */
    public String getNodeTopic(String edgeNode) {
        assert edgeNode != null : "Edge node name must not be null";
        return String.format("%s/%s/%s/%s", NAMESPACE, GROUP_ID, topicType, edgeNode);
    }

}
