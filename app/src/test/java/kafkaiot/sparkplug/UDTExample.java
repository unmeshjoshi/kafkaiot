package kafkaiot.sparkplug;

import org.eclipse.tahu.SparkplugException;
import org.eclipse.tahu.message.model.Metric;
import org.eclipse.tahu.message.model.Metric.MetricBuilder;
import org.eclipse.tahu.message.model.Template;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import org.eclipse.tahu.message.model.*;

import static org.eclipse.tahu.message.model.Template.*;


public class UDTExample {

    // Create a UDT for the Chassis Assembly Station
    public Template createChassisAssemblyUDT() throws SparkplugException {
        List<Metric> metrics = new ArrayList<>();
        metrics.add(new MetricBuilder("temperature", MetricDataType.Float, 20.5f).createMetric());
        metrics.add(new MetricBuilder("pressure", MetricDataType.Float, 101.3f).createMetric());
        metrics.add(new MetricBuilder("status", MetricDataType.String, "Operational").createMetric());

        return new TemplateBuilder()
                .version("v1.0")
                .templateRef("ChassisAssembly")
                .definition(true)
                .addMetrics(metrics)
                .createTemplate();
    }

    // Create UDT for Robot Arm
    public Template createRobotArmUDT() throws SparkplugException {
        List<Metric> metrics = new ArrayList<>();
        metrics.add(new MetricBuilder("positionX", MetricDataType.Float, 12.5f).createMetric());
        metrics.add(new MetricBuilder("positionY", MetricDataType.Float, 7.8f).createMetric());
        metrics.add(new MetricBuilder("status", MetricDataType.String, "Idle").createMetric());

        return new TemplateBuilder()
                .version("v1.0")
                .templateRef("RobotArm")
                .definition(true)
                .addMetrics(metrics)
                .createTemplate();
    }

    // Create UDT for Welding Machine
    public Template createWeldingMachineUDT() throws SparkplugException {
        List<Metric> metrics = new ArrayList<>();
        metrics.add(new MetricBuilder("temperature", MetricDataType.Float, 1500.0f).createMetric());
        metrics.add(new MetricBuilder("weldCount", MetricDataType.Int32, 100).createMetric());
        metrics.add(new MetricBuilder("status", MetricDataType.String, "Active").createMetric());

        return new TemplateBuilder()
                .version("v1.0")
                .templateRef("WeldingMachine")
                .definition(true)
                .addMetrics(metrics)
                .createTemplate();
    }

    // Create UDT for Conveyor Belt
    public Template createConveyorBeltUDT() throws SparkplugException {
        List<Metric> metrics = new ArrayList<>();
        metrics.add(new MetricBuilder("speed", MetricDataType.Float, 2.5f).createMetric());
        metrics.add(new MetricBuilder("status", MetricDataType.String, "Running").createMetric());

        return new TemplateBuilder()
                .version("v1.0")
                .templateRef("ConveyorBelt")
                .definition(true)
                .addMetrics(metrics)
                .createTemplate();
    }

    // Create UDT for Sensor
    public Template createSensorUDT() throws SparkplugException {
        List<Metric> metrics = new ArrayList<>();
        metrics.add(new MetricBuilder("sensorValue", MetricDataType.Float, 45.6f).createMetric());
        metrics.add(new MetricBuilder("status", MetricDataType.String, "Normal").createMetric());

        return new TemplateBuilder()
                .version("v1.0")
                .templateRef("Sensor")
                .definition(true)
                .addMetrics(metrics)
                .createTemplate();
    }

}
