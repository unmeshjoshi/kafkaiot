package kafkaiot.sparkplug;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.eclipse.tahu.message.model.Metric;
import org.eclipse.tahu.message.model.MetricDataType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AvroSchemaMapper {

    // Initialize a map to hold the Sparkplug-to-Avro type mappings
    private static final Map<MetricDataType, Schema.Type> typeMapping = new HashMap<>();

    static {
        // Populate the map with the mappings
        typeMapping.put(MetricDataType.Int8, Schema.Type.INT);
        typeMapping.put(MetricDataType.Int16, Schema.Type.INT);
        typeMapping.put(MetricDataType.Int32, Schema.Type.INT);
        typeMapping.put(MetricDataType.Int64, Schema.Type.LONG);
        typeMapping.put(MetricDataType.UInt8, Schema.Type.INT); // Unsigned handling as closest possible
        typeMapping.put(MetricDataType.UInt16, Schema.Type.INT);
        typeMapping.put(MetricDataType.UInt32, Schema.Type.LONG);
        typeMapping.put(MetricDataType.UInt64, Schema.Type.LONG);
        typeMapping.put(MetricDataType.Float, Schema.Type.FLOAT);
        typeMapping.put(MetricDataType.Double, Schema.Type.DOUBLE);
        typeMapping.put(MetricDataType.Boolean, Schema.Type.BOOLEAN);
        typeMapping.put(MetricDataType.String, Schema.Type.STRING);
        typeMapping.put(MetricDataType.DateTime, Schema.Type.LONG); // Representing as Avro long (timestamp)
        typeMapping.put(MetricDataType.Text, Schema.Type.STRING);
        typeMapping.put(MetricDataType.UUID, Schema.Type.STRING); // UUID as String in Avro
    }

    public Schema generateAvroSchemaFromTemplate(List<Metric> metrics, String templateRef) {
        // Start building the Avro schema
        SchemaBuilder.RecordBuilder<Schema> recordBuilder = SchemaBuilder.record(templateRef);

        SchemaBuilder.FieldAssembler<Schema> fieldAssembler = recordBuilder.fields();

        // Get metrics from the template

        // Add each metric as a field to the Avro schema
        for (Metric metric : metrics) {
            fieldAssembler = addField(fieldAssembler, metric);
        }

        return fieldAssembler.endRecord();
    }

    private SchemaBuilder.FieldAssembler<Schema> addField(SchemaBuilder.FieldAssembler<Schema> fieldAssembler, Metric metric) {
        String metricName = sanitizeFieldName(metric.getName());
        Schema avroType = mapSparkplugTypeToAvro(metric);

        // Add the field to the schema
        return fieldAssembler.name(metricName).type(avroType).noDefault();
    }

    // Method to sanitize field names to conform with Avro naming conventions
    private String sanitizeFieldName(String originalName) {
        // Replace non-alphanumeric characters with underscores
        String sanitized = originalName.replaceAll("[^A-Za-z0-9_]", "_");

        // Ensure the first character is a letter or underscore
        if (!sanitized.matches("^[A-Za-z_].*")) {
            sanitized = "_" + sanitized;
        }

        return sanitized;
    }

    protected Schema mapSparkplugTypeToAvro(Metric metric) {
        MetricDataType dataType = metric.getDataType();
        Schema.Type avroType = typeMapping.getOrDefault(dataType, Schema.Type.STRING); // Default to STRING if not found

        return Schema.create(avroType);
    }
}
