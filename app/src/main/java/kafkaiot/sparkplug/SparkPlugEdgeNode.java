package kafkaiot.sparkplug;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.tahu.SparkplugException;
import org.eclipse.tahu.message.SparkplugBPayloadEncoder;
import org.eclipse.tahu.message.model.Metric;
import org.eclipse.tahu.message.model.SparkplugBPayload;
import org.eclipse.tahu.message.model.Template;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static org.eclipse.tahu.message.model.MetricDataType.Boolean;
import static org.eclipse.tahu.message.model.MetricDataType.Int64;

public class SparkPlugEdgeNode {

    private final String edgeNodeName;
    private final Template template;
    private final List<SparkPlugDevice> devices;
    private final MqttClient mqttClient;

    public SparkPlugEdgeNode(String edgeNodeName, Template template,
                             String mqttListenAddress) throws MqttException {
        this.edgeNodeName = edgeNodeName;
        this.template = template;

        this.mqttClient = MqttUtils.createMqttClient(mqttListenAddress);
        this.devices = new ArrayList<>();
    }

    public void sendSparkplugMessages() throws SparkplugException {
        sendNBIRTH();
        sendNDATA();
        sendDeviceMessages();
    }

    public String getEdgeNodeName() {
        return edgeNodeName;
    }

    public void addDevice(SparkPlugDevice device) {
        devices.add(device);
    }

    public void sendNBIRTH() throws SparkplugException {
        Template nbirthTemplate = createNBIRHTTemplate();

        nbirthTemplate.addMetric(new Metric.MetricBuilder("bdSeq", Int64, (long) bdSeq).createMetric());
        nbirthTemplate.addMetric(new Metric.MetricBuilder("Node Control/Rebirth", Boolean, false).createMetric());

        publishMessage(SparkPlugBTopic.NBIRTH.getNodeTopic(edgeNodeName),
                nbirthTemplate);
    }

    public void sendNDATA() throws SparkplugException {
        // Example NDATA message construction
        Template ndataTemplate = createNDATATemplate();
        publishMessage(SparkPlugBTopic.NDATA.getNodeTopic(edgeNodeName), ndataTemplate);
    }


    public void sendDeviceMessages() {
        sendDeviceBirthMessages();
        for (int i = 0; i < 10; i++) {
            sendDeviceDataMessages(); //TDODO: do this periodically
        }
    }

    private void sendDeviceDataMessages() {
        for (SparkPlugDevice device : devices) {
            SparkPlugDevice.DeviceMessage ddata = device.getDDATA(edgeNodeName);
            publishMessage(ddata.topic, ddata.template);
        }

    }

    private void sendDeviceBirthMessages() {
        for (SparkPlugDevice device : devices) {
            sendDBirth(device);
        }
    }

    private void sendDBirth(SparkPlugDevice device) {
        SparkPlugDevice.DeviceMessage dbirth = device.createDBIRTH(edgeNodeName);
        publishMessage(dbirth.topic, dbirth.template);
    }

    private Template createNBIRHTTemplate() throws SparkplugException {
        // Logic to create an NBIRTH UDT Template
        return new Template.TemplateBuilder()
                .version("v1.0")
                .templateRef("EdgeNode_" + edgeNodeName)
                .definition(true)
                .createTemplate();
    }

    private Template createNDATATemplate() throws SparkplugException {
        // Logic to create an NDATA UDT Template
        return new Template.TemplateBuilder()
                .version("v1.0")
                .templateRef("EdgeNode_" + edgeNodeName)
                .definition(false)
                .createTemplate();
    }

    private void publishMessage(String topic, Template template) {
        try {
            System.out.println("Publishing to topic: " + topic);
            System.out.println("Message: " + template);
            // Actual publish code would go here
            SparkplugBPayload payload = new SparkplugBPayload(new Date(), template.getMetrics(),
                    incrementAndGetSequenceNumber(), newUUID(), null); //TODO:Why is body null?

            mqttClient.publish(topic, new SparkplugBPayloadEncoder().getBytes(payload, false), 0, false);

        } catch(Exception e) {
            throw new RuntimeException(e);
        }
    }

    private int bdSeq = 0; //When to increment this?
    private int seq = 0;

    // Used to add the sequence number
    private long incrementAndGetSequenceNumber() throws Exception {
        System.out.println("seq: " + seq);
        if (seq == 256) {
            seq = 0;
        }
        return seq++;
    }

    private String newUUID() {
        return java.util.UUID.randomUUID().toString();
    }

    public List<SparkPlugDevice> getDevices() {
        return devices;
    }
}
