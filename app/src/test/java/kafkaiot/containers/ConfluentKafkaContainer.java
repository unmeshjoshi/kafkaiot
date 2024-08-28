package kafkaiot.containers;

import org.jetbrains.annotations.NotNull;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import org.testcontainers.utility.DockerImageName;

public class ConfluentKafkaContainer {
    public static final String SCHEMA_REGISTRY_IMAGE =
            "confluentinc/cp-schema-registry";
    public static final int SCHEMA_REGISTRY_PORT = 8081;
    private GenericContainer schemaRegistryContainer;
    private KafkaContainer kafkaContainer;

    public void start() {
        Network kafkaNetwork = Network.newNetwork();

        kafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.3.9"));
        kafkaContainer.withNetwork(kafkaNetwork);
        kafkaContainer.start();

        schemaRegistryContainer =
                new GenericContainer(DockerImageName.parse(SCHEMA_REGISTRY_IMAGE +
                        ":" + "7.3.9"))
                        .withNetwork(kafkaNetwork)
                        .withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
                        .withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
                        .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS",
                                "PLAINTEXT://" + kafkaContainer.getNetworkAliases().get(0) + ":9092")
                        .withExposedPorts(SCHEMA_REGISTRY_PORT)
                        .waitingFor(new HttpWaitStrategy().forPath("/subjects").forStatusCode(200));

        schemaRegistryContainer.start();
    }

    @NotNull
    public String getSchemaRegistryUrl() {
        String schemaRegistryUrl =
                "http://" + schemaRegistryContainer.getHost() + ":" + schemaRegistryContainer.getMappedPort(8081);
        return schemaRegistryUrl;
    }

    public void stop() {
        schemaRegistryContainer.stop();
        kafkaContainer.stop();
    }

    public String getBootstrapServers() {
        return kafkaContainer.getBootstrapServers();
    }
}
