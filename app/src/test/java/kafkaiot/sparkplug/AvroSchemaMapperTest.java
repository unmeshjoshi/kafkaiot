package kafkaiot.sparkplug;

import org.apache.avro.Schema;
import org.eclipse.tahu.SparkplugInvalidTypeException;
import org.eclipse.tahu.message.model.Metric;
import org.eclipse.tahu.message.model.Metric.MetricBuilder;
import org.eclipse.tahu.message.model.Template;
import org.eclipse.tahu.message.model.MetricDataType;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class AvroSchemaMapperTest {

    private AvroSchemaMapper avroSchemaGenerator;

    @Before
    public void setUp() {
        avroSchemaGenerator = new AvroSchemaMapper();
    }

    @Test
    public void testGenerateAvroSchemaFromTemplate() throws SparkplugInvalidTypeException {
        Template template = createTestTemplate();
        Schema avroSchema = avroSchemaGenerator.generateAvroSchemaFromTemplate(template.getMetrics(), template.getTemplateRef());

        assertNotNull("Avro schema should not be null", avroSchema);
        assertEquals("Schema name should match template reference", "ChassisAssembly", avroSchema.getName());

        // Validate fields
        Schema.Field tempField = avroSchema.getField("temperature");
        assertNotNull("Temperature field should be present", tempField);
        assertEquals("Temperature field type should be FLOAT", Schema.Type.FLOAT, tempField.schema().getType());

        Schema.Field pressureField = avroSchema.getField("pressure");
        assertNotNull("Pressure field should be present", pressureField);
        assertEquals("Pressure field type should be FLOAT", Schema.Type.FLOAT, pressureField.schema().getType());

        Schema.Field statusField = avroSchema.getField("status");
        assertNotNull("Status field should be present", statusField);
        assertEquals("Status field type should be STRING", Schema.Type.STRING, statusField.schema().getType());

        // Validate sanitized fields
        Schema.Field nodeControlRebirthField = avroSchema.getField("Node_Control_Rebirth");
        assertNotNull("Node_Control_Rebirth field should be present", nodeControlRebirthField);
        assertEquals("Node_Control_Rebirth field type should be STRING", Schema.Type.STRING, nodeControlRebirthField.schema().getType());
    }

    @Test
    public void testMetricTypeMapping() throws SparkplugInvalidTypeException {
        Metric int8Metric = new MetricBuilder("int8Metric", MetricDataType.Int8, (byte) 1).createMetric();
        Schema int8Schema = avroSchemaGenerator.mapSparkplugTypeToAvro(int8Metric);
        assertEquals("Int8 type should map to INT", Schema.Type.INT, int8Schema.getType());

        Metric floatMetric = new MetricBuilder("floatMetric", MetricDataType.Float, 1.0f).createMetric();
        Schema floatSchema = avroSchemaGenerator.mapSparkplugTypeToAvro(floatMetric);
        assertEquals("Float type should map to FLOAT", Schema.Type.FLOAT, floatSchema.getType());

        Metric stringMetric = new MetricBuilder("stringMetric", MetricDataType.String, "test").createMetric();
        Schema stringSchema = avroSchemaGenerator.mapSparkplugTypeToAvro(stringMetric);
        assertEquals("String type should map to STRING", Schema.Type.STRING, stringSchema.getType());
    }

    private Template createTestTemplate() throws SparkplugInvalidTypeException {
        List<Metric> metrics = new ArrayList<>();
        metrics.add(new MetricBuilder("temperature", MetricDataType.Float, 20.5f).createMetric());
        metrics.add(new MetricBuilder("pressure", MetricDataType.Float, 101.3f).createMetric());
        metrics.add(new MetricBuilder("status", MetricDataType.String, "Operational").createMetric());
        metrics.add(new MetricBuilder("Node Control/Rebirth", MetricDataType.String, "Rebirth").createMetric());

        return new Template.TemplateBuilder()
                .version("v1.0")
                .templateRef("ChassisAssembly")
                .definition(true)
                .addMetrics(metrics)
                .createTemplate();
    }
}
