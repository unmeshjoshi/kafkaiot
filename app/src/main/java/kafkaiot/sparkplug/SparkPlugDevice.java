package kafkaiot.sparkplug;

import org.eclipse.tahu.SparkplugException;
import org.eclipse.tahu.message.model.Template;
import org.eclipse.tahu.protobuf.SparkplugBProto;

public class SparkPlugDevice {

    private final String deviceName;
    private final Template deviceTemplate;

    public SparkPlugDevice(String deviceName, Template deviceTemplate) {
        this.deviceName = deviceName;
        this.deviceTemplate = deviceTemplate;
    }

    public String getDeviceName() {
        return deviceName;
    }

    public Template getDeviceTemplate() {
        return deviceTemplate;
    }

    // Method to build and send DBIRTH message
    public void sendDBIRTH() throws SparkplugException {
        // Example DBIRTH message construction
        Template dbirthTemplate = createDBIRTHTemplate();
        // Publish DBIRTH message
        publishMessage(SparkPlugBTopic.DBIRTH.getDeviceTopic("ChassisAssembly",
                deviceName), dbirthTemplate);
    }

    // Method to build and send DDATA message
    public void sendDDATA() throws SparkplugException {
        // Example DDATA message construction
        Template ddataTemplate = createDDATATemplate();
        // Publish DDATA message
        publishMessage(SparkPlugBTopic.DDATA.getDeviceTopic("ChassisAssembly",
                deviceName), ddataTemplate);
    }

    private Template createDBIRTHTemplate() throws SparkplugException {
        // Logic to create a DBIRTH UDT Template
        return new Template.TemplateBuilder()
                .version("v1.0")
                .templateRef(deviceName)
                .definition(true)
                .addMetrics(deviceTemplate.getMetrics()) // Assume we reuse metrics for simplicity
                .createTemplate();
    }

    private Template createDDATATemplate() throws SparkplugException {
        // Logic to create a DDATA UDT Template
        return new Template.TemplateBuilder()
                .version("v1.0")
                .templateRef(deviceName)
                .definition(false) // Not a definition, so false
                .addMetrics(deviceTemplate.getMetrics())
                .createTemplate();
    }

    private void publishMessage(String topic, Template template) {
        // Logic to publish message over MQTT or other protocol
        System.out.println("Publishing to topic: " + topic);
        System.out.println("Message: " + template);
        // Actual publish code would go here
    }
}
