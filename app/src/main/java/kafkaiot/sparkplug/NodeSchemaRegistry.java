package kafkaiot.sparkplug;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.avro.Schema;
import org.eclipse.tahu.message.model.SparkplugBPayload;

import java.io.IOException;

public class NodeSchemaRegistry {
    private final AvroSchemaMapper avroSchemaMapper = new AvroSchemaMapper();
    private final CachedSchemaRegistryClient schemaRegistryClient;

    public NodeSchemaRegistry(String schemaRegistryUrl) {
        schemaRegistryClient
                = new CachedSchemaRegistryClient(schemaRegistryUrl,
                100);
    }

    public int registerSchema(String name, SparkplugBPayload inboundPayload) throws RestClientException, IOException {
        Schema schema = avroSchemaMapper.generateAvroSchemaFromTemplate(inboundPayload.getMetrics(), name);
        return register(name, schema);
    }

    public int register(String name, Schema schema) throws IOException,
            RestClientException {
        return schemaRegistryClient.register(valueSchemaSubject(name), schema);
    }


    private static String valueSchemaSubject(String name) {
        return name + "-value";
    }

    public boolean hasRegisteredSchemaFor(String deviceName) throws RestClientException, IOException {
        return schemaRegistryClient.getAllVersions(deviceName)
                .size() > 0;
    }

    public SchemaMetadata getLatestSchemaMetadata(String deviceNodeName) throws RestClientException, IOException {
        return schemaRegistryClient.getLatestSchemaMetadata(valueSchemaSubject(deviceNodeName));
    }
}
