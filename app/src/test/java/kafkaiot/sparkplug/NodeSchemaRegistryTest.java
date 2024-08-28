package kafkaiot.sparkplug;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import kafkaiot.containers.ConfluentKafkaContainer;
import org.apache.avro.Schema;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class NodeSchemaRegistryTest {

    private ConfluentKafkaContainer kafkaContainer;

    @Before
    public void startContainers() {
        kafkaContainer = new ConfluentKafkaContainer();
        kafkaContainer.start();
    }

    @After
    public void stopContainers() {
        kafkaContainer.stop();
    }

    @Test
    public void registerSchema() throws RestClientException, IOException {
        NodeSchemaRegistry registry =
                new NodeSchemaRegistry(kafkaContainer.getSchemaRegistryUrl());


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


        registry.register("deviceName", version2.rawSchema());


        AvroSchema version3 = createAvroSchema("{\"type\":\"record\",\"name\":\"RobotArm\",\"fields\":[{\"name\":\"positionX\",\"type\":\"float\"},{\"name\":\"positionY\",\"type\":\"float\"},{\"name\":\"status\",\"type\":\"string\"}]}");
        registry.register("RobotArm-value", version3.rawSchema());

        AvroSchema address = createAvroSchema("{"
                + "\"type\": \"record\","
                + "\"name\": \"Address\","
                + "\"fields\": ["
                + "{\"name\": \"name\", \"type\": \"string\"},"
                + "{\"name\": \"age\", \"type\": \"int\"},"
                + "{\"name\": \"email\", \"type\": \"string\"},"
                + "{\"name\": \"phone\", \"type\": \"string\", " +
                "\"default\": \"null\"}"
                + "]"
                + "}");

        int register = registry.register("address-value", address.rawSchema());
        assertEquals(3, register);
    }

    @NotNull
    private static AvroSchema createAvroSchema(String schemaString) {
        Schema schema = new Schema.Parser().parse(schemaString);
        AvroSchema avroSchema = new AvroSchema(schema);
        return avroSchema;
    }




}