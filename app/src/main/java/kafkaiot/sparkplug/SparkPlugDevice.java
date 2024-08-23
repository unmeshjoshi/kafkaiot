package kafkaiot.sparkplug;

import org.eclipse.tahu.message.model.Template;

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

    class DeviceMessage {
        String topic;
        Template template;

        public DeviceMessage(String topic, Template template) {
            this.topic = topic;
            this.template = template;
        }
    }

    public DeviceMessage createDBIRTH() {
        return new DeviceMessage(SparkPlugBTopic.DBIRTH.getDeviceTopic("ChassisAssembly",
                deviceName), createDBIRTHTemplate());
    }

    public DeviceMessage getDDATA() {
        return new DeviceMessage(SparkPlugBTopic.DDATA.getDeviceTopic("ChassisAssembly",
                deviceName), createDDATATemplate());
    }

    private Template createDBIRTHTemplate() {
        // Logic to create a DBIRTH UDT Template
        return new Template.TemplateBuilder()
                .version("v1.0")
                .templateRef(deviceName)
                .definition(true)
                .addMetrics(deviceTemplate.getMetrics())
                .createTemplate();
    }

    private Template createDDATATemplate() {
        // Logic to create a DDATA UDT Template
        return new Template.TemplateBuilder()
                .version("v1.0")
                .templateRef(deviceName)
                .definition(false) // Not a definition, so false
                .addMetrics(deviceTemplate.getMetrics())
                .createTemplate();
    }
}
