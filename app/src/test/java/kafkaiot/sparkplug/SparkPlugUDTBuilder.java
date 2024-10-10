package kafkaiot.sparkplug;

import com.fasterxml.jackson.core.type.TypeReference;
import org.eclipse.tahu.SparkplugException;
import org.eclipse.tahu.message.model.Metric;
import org.eclipse.tahu.message.model.MetricDataType;
import org.eclipse.tahu.message.model.Template;
import org.eclipse.tahu.message.model.Template.TemplateBuilder;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SparkPlugUDTBuilder {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public Template createTemplateFromJson(String jsonString) throws SparkplugException {
        try {
            // Parse JSON into a Map
            TypeReference<HashMap<String,Object>> typeRef
                    = new TypeReference<>() {
            };

            Map<String, Object> templateMap =
                    objectMapper.readValue(jsonString, typeRef);
            String version = (String) templateMap.get("version");
            String templateRef = (String) templateMap.get("templateRef");
            boolean definition = (Boolean) templateMap.get("definition");
            List<Map<String, Object>> metricsList = (List<Map<String, Object>>) templateMap.get("metrics");

            TemplateBuilder templateBuilder = new TemplateBuilder()
                    .version(version)
                    .templateRef(templateRef)
                    .definition(definition);

            List<Metric> metrics = new ArrayList<>();
            for (Map<String, Object> metricEntry : metricsList) {
                String name = (String) metricEntry.get("name");
                String type = (String) metricEntry.get("type");
                Object value = metricEntry.get("value");

                MetricDataType dataType = getMetricDataType(type);
                Metric metric = new Metric.MetricBuilder(name, dataType, value).createMetric();
                metrics.add(metric);
            }

            return templateBuilder.addMetrics(metrics).createTemplate();
        } catch (Exception e) {
            throw new SparkplugException("Error creating template from JSON", e);
        }
    }

    private MetricDataType getMetricDataType(String type) {
        switch (type) {
            case "Float":
                return MetricDataType.Float;
            case "Double":
                return MetricDataType.Double;
            case "Int32":
                return MetricDataType.Int32;
            case "String":
                return MetricDataType.String;
            // Add more cases as needed
            default:
                throw new IllegalArgumentException("Unknown metric type: " + type);
        }
    }
}
