package distsearch;

import kafka.utils.TestUtils;
import org.apache.lucene.document.Document;
import org.junit.Test;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class LuceneSearchTest {

    @Test
    public void testIndexingWithControlledClock() throws Exception {
        TestClock clock = new TestClock(Instant.parse("2023-06-01T00:00:00Z"));
        IndexService indexService = new IndexService(clock,"test_index", TestUtils.tempDir().getAbsolutePath(), 1);

        String json1 = "{\"_id\":\"1\",\"title\":\"Test Document 1\",\"content\":\"This is a test document\"}";
        indexService.indexDocument(json1);

        clock.advanceTime(1, ChronoUnit.HOURS);

        String json2 = "{\"_id\":\"2\",\"title\":\"Test Document 2\",\"content\":\"This is another test document\"}";
        indexService.indexDocument(json2);

        List<Document> results = indexService.searchDateRange("@timestamp",
                Instant.parse("2023-06-01T00:00:00Z"),
                Instant.parse("2023-06-01T00:30:00Z"));

        assertEquals(1, results.size());
        assertEquals("1", results.get(0).get("_id"));

        indexService.close();
    }
}
