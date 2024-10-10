package kafkaiot.sparkplug;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificRecordBase;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;

import java.util.List;
public class GenericRecord2JsonMapper {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static String toJson(GenericRecord record) {
        try {
            ObjectNode jsonNode = objectMapper.createObjectNode();
            Schema schema = record.getSchema();
            List<Field> fields = schema.getFields();

            for (Field field : fields) {
                String fieldName = field.name();
                Object value = record.get(fieldName);

                if (value == null) {
                    jsonNode.putNull(fieldName);
                } else if (value instanceof String) {
                    jsonNode.put(fieldName, (String) value);
                } else if (value instanceof Integer) {
                    jsonNode.put(fieldName, (Integer) value);
                } else if (value instanceof Long) {
                    jsonNode.put(fieldName, (Long) value);
                } else if (value instanceof Float) {
                    jsonNode.put(fieldName, (Float) value);
                } else if (value instanceof Double) {
                    jsonNode.put(fieldName, (Double) value);
                } else if (value instanceof Boolean) {
                    jsonNode.put(fieldName, (Boolean) value);
                } else if (value instanceof GenericRecord) {
                    jsonNode.set(fieldName, objectMapper.readTree(toJson((GenericRecord) value)));
                } else if (value instanceof List) {
                    jsonNode.set(fieldName, objectMapper.valueToTree(value));
                } else {
                    jsonNode.put(fieldName, value.toString());
                }
            }

            return objectMapper.writeValueAsString(jsonNode);
        } catch (Exception e) {
            throw new RuntimeException("Error converting GenericRecord to JSON", e);
        }
    }
}
