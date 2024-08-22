package kafkaiot;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;

public class SchemaRegistryContainer extends GenericContainer<SchemaRegistryContainer> {
    public static final String SCHEMA_REGISTRY_IMAGE =
            "confluentinc/cp-schema-registry";
    public static final int SCHEMA_REGISTRY_PORT = 8081;

    public SchemaRegistryContainer() {
        super(SCHEMA_REGISTRY_IMAGE + ":" + "7.3.9");

        waitingFor(new HttpWaitStrategy().forPath("/subjects").forStatusCode(200));
        withExposedPorts(SCHEMA_REGISTRY_PORT);
    }
}
