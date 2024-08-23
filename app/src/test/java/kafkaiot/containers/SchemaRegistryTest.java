package kafkaiot.containers;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import kafkaiot.containers.ConfluentKafkaContainer;
import org.apache.avro.Schema;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class SchemaRegistryTest {

    ConfluentKafkaContainer container;
    @Before
    public void startContainers() {

        container = new ConfluentKafkaContainer();
        container.start();
    }

    @After
    public void stopContainers() {
        container.stop();
    }

    @Test
    public void testRegisterTwoVersions() throws RestClientException,
            IOException {
        String schemaRegistryUrl = container.getSchemaRegistryUrl();
        SchemaRegistryClient client = new CachedSchemaRegistryClient(schemaRegistryUrl, 100);

        AvroSchema version1 = createAvroSchema("{"
                + "\"type\": \"record\","
                + "\"name\": \"User\","
                + "\"fields\": ["
                + "{\"name\": \"name\", \"type\": \"string\"},"
                + "{\"name\": \"age\", \"type\": \"int\"},"
                + "{\"name\": \"email\", \"type\": \"string\"}"
                + "]"
                + "}");

        int schemaId = client.register("User", version1);

        assertEquals(1, schemaId);

        AvroSchema version2 = createAvroSchema("{"
                + "\"type\": \"record\","
                + "\"name\": \"User\","
                + "\"fields\": ["
                + "{\"name\": \"name\", \"type\": \"string\"},"
                + "{\"name\": \"age\", \"type\": \"int\"},"
                + "{\"name\": \"email\", \"type\": \"string\"},"
                + "{\"name\": \"phone\", \"type\": \"string\", " +
                "\"default\": \"null\"}"
                + "]"
                + "}");

        int schemaId2 = client.register("User", version2);
        assertEquals(2, schemaId2);


        List<Integer> userSchemaVersions = client.getAllVersions("User");
        assertEquals(Arrays.asList(1, 2), userSchemaVersions);
    }

    @NotNull
    private static AvroSchema createAvroSchema(String schemaString) {
        Schema schema = new Schema.Parser().parse(schemaString);
        AvroSchema avroSchema = new AvroSchema(schema);
        return avroSchema;
    }
}
