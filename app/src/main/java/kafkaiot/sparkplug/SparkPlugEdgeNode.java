package kafkaiot.sparkplug;

import org.eclipse.tahu.SparkplugException;
import org.eclipse.tahu.message.model.Template;

import java.util.ArrayList;
import java.util.List;

public class SparkPlugEdgeNode {

    private final String edgeNodeName;
    private final Template template;
    private final List<SparkPlugDevice> devices;

    public SparkPlugEdgeNode(String edgeNodeName, Template template) {
        this.edgeNodeName = edgeNodeName;
        this.template = template;
        this.devices = new ArrayList<>();
    }

    public String getEdgeNodeName() {
        return edgeNodeName;
    }

    public void addDevice(SparkPlugDevice device) {
        devices.add(device);
    }

    public void sendNBIRTH() throws SparkplugException {
        // Example NBIRTH message construction
        Template nbirthTemplate = createNBIRHTTemplate();
        // Publish NBIRTH message
        publishMessage(SparkPlugBTopic.NBIRTH.getNodeTopic(edgeNodeName),
                nbirthTemplate);
    }

    public void sendNDATA() throws SparkplugException {
        // Example NDATA message construction
        Template ndataTemplate = createNDATATemplate();
        // Publish NDATA message
        publishMessage(SparkPlugBTopic.NDATA.getNodeTopic(edgeNodeName), ndataTemplate);
    }

    public void sendDeviceMessages() throws SparkplugException {
        for (SparkPlugDevice device : devices) {
            device.sendDBIRTH();
            device.sendDDATA();
        }
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
        // Logic to publish message over MQTT or other protocol
        System.out.println("Publishing to topic: " + topic);
        System.out.println("Message: " + template);
        // Actual publish code would go here
    }
}
