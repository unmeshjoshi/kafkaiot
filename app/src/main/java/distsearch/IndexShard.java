package distsearch;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.IOException;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.*;

public class IndexShard implements AutoCloseable {
    private final String shardId;
    private final Directory directory;
    private final IndexWriter writer;
    private final SearcherManager searcherManager;
    private final Clock clock;

    public IndexShard(Clock clock, String indexName, String indexPath) throws IOException, NoSuchAlgorithmException {
        this.clock = clock;
        this.shardId = generateShardId(indexName);
        this.directory = FSDirectory.open(Paths.get(indexPath));
        IndexWriterConfig config = new IndexWriterConfig(new StandardAnalyzer());
        this.writer = new IndexWriter(directory, config);
        this.searcherManager = new SearcherManager(writer, null);
    }


    private String generateShardId(String indexName) throws NoSuchAlgorithmException {
        long timestamp = clock.now().toEpochMilli();
        String uniqueString = indexName + "-" + timestamp + "-" + Thread.currentThread().getId();
        MessageDigest md = MessageDigest.getInstance("SHA-256");
        byte[] digest = md.digest(uniqueString.getBytes());
        return Base64.getUrlEncoder().encodeToString(digest).substring(0, 20);
    }

    public String getShardId() {
        return shardId;
    }

    public void indexDocument(String json) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode jsonNode = mapper.readTree(json);
        Document doc = jsonToLuceneDocument(jsonNode);
        writer.addDocument(doc);
        writer.commit();
        searcherManager.maybeRefresh();
    }

    public List<Document> search(String field, String queryString) throws IOException {
        List<Document> results = new ArrayList<>();
        IndexSearcher searcher = searcherManager.acquire();
        try {
            Query query = new TermQuery(new Term(field, queryString));
            TopDocs topDocs = searcher.search(query, 10);
            for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
                results.add(searcher.doc(scoreDoc.doc));
            }
        } finally {
            searcherManager.release(searcher);
        }
        return results;
    }

    private Document jsonToLuceneDocument(JsonNode jsonNode) {
        Document doc = new Document();

        // Add @timestamp field
        long timestamp = clock.now().toEpochMilli();
        doc.add(new LongPoint("@timestamp", timestamp));
        doc.add(new NumericDocValuesField("@timestamp", timestamp));
        doc.add(new StoredField("@timestamp", timestamp));

        // Process each field in the JSON
        Iterator<Map.Entry<String, JsonNode>> fields = jsonNode.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> entry = fields.next();
            String fieldName = entry.getKey();
            JsonNode fieldValue = entry.getValue();

            if (fieldName.equals("_id")) {
                doc.add(new StringField("_id", fieldValue.asText(), Field.Store.YES));
            } else if (fieldValue.isTextual()) {
                doc.add(new TextField(fieldName, fieldValue.asText(), Field.Store.YES));
            } else if (fieldValue.isNumber()) {
                doc.add(new LongPoint(fieldName, fieldValue.asLong()));
                doc.add(new NumericDocValuesField(fieldName, fieldValue.asLong()));
                doc.add(new StoredField(fieldName, fieldValue.asLong()));
            } else if (fieldValue.isArray()) {
                for (JsonNode arrayElement : fieldValue) {
                    doc.add(new StringField(fieldName, arrayElement.asText(), Field.Store.YES));
                }
            }
            // Add more type checks as needed
        }

        return doc;
    }

    public List<Document> searchDateRange(String field, long startDate, long endDate) throws IOException {
        List<Document> results = new ArrayList<>();
        IndexSearcher searcher = searcherManager.acquire();
        try {
            Query rangeQuery = LongPoint.newRangeQuery(field, startDate, endDate);
            TopDocs topDocs = searcher.search(rangeQuery, 100); // Adjust the limit as needed
            for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
                Document doc = searcher.doc(scoreDoc.doc);
                // Convert the stored long value back to a readable date string
                String dateValue = Instant.ofEpochMilli(Long.parseLong(doc.get(field))).toString();
                doc.removeField(field);
                doc.add(new StringField(field, dateValue, Field.Store.YES));
                results.add(doc);
            }
        } finally {
            searcherManager.release(searcher);
        }
        return results;
    }
    @Override
    public void close() throws IOException {
        searcherManager.close();
        writer.close();
        directory.close();
    }
}
