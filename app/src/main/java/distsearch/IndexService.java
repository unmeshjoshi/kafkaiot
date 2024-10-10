package distsearch;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.lucene.document.Document;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;


public class IndexService implements AutoCloseable {
    private final List<IndexShard> shards;
    private final int numberOfShards;
    private final String indexName;

    public IndexService(Clock clock, String indexName, String basePath, int numberOfShards) throws IOException, NoSuchAlgorithmException {
        this.indexName = indexName;
        this.numberOfShards = numberOfShards;
        this.shards = new ArrayList<>(numberOfShards);

        for (int i = 0; i < numberOfShards; i++) {
            String shardPath = basePath + "/shard_" + i;
            IndexShard shard = new IndexShard(clock, indexName, shardPath);
            shards.add(shard);
            System.out.println("Created shard " + i + " with ID: " + shard.getShardId());
        }
    }

    public void indexDocument(String json) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode jsonNode = mapper.readTree(json);
        String id = jsonNode.has("_id") ? jsonNode.get("_id").asText() : UUID.randomUUID().toString();
        int shardNumber = selectShard(id);
        shards.get(shardNumber).indexDocument(json);
    }

    public List<Document> search(String field, String queryString) throws IOException {
        List<Document> results = new ArrayList<>();
        for (IndexShard shard : shards) {
            results.addAll(shard.search(field, queryString));
        }
        return results;
    }

    private int selectShard(String routingKey) {
        return Math.floorMod(hash(routingKey), numberOfShards);
    }

    private int hash(String routing) {
        return Math.abs(routing.hashCode());
    }

    @Override
    public void close() throws IOException {
        for (IndexShard shard : shards) {
            shard.close();
        }
    }

    public List<Document> searchDateRange(String field, Instant startDate, Instant endDate) throws IOException {
        List<Document> results = new ArrayList<>();
        for (IndexShard shard : shards) {
            results.addAll(shard.searchDateRange(field, startDate.toEpochMilli(), endDate.toEpochMilli()));
        }
        return results;
    }
}

