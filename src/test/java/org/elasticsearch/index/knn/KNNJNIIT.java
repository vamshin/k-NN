package org.elasticsearch.index.knn;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FilterDirectory;
import org.elasticsearch.test.ESIntegTestCase;

import java.nio.file.Paths;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

//@Ignore
@ESIntegTestCase.ClusterScope(scope=ESIntegTestCase.Scope.SUITE, numDataNodes=1)
public class KNNJNIIT extends ESIntegTestCase {
    private static final Logger logger = LogManager.getLogger(KNNJNIIT.class);

    public void testCreateHnswIndex() throws Exception {
        int[] docs = {0, 1, 2};

        float[][] vectors = {
                {1.0f, 2.0f, 3.0f, 4.0f},
                {5.0f, 6.0f, 7.0f, 8.0f},
                {9.0f, 10.0f, 11.0f, 12.0f}
        };

        Directory dir = newFSDirectory(createTempDir());
        String segmentName = "_dummy";
        String indexPath = Paths.get(((FSDirectory) (FilterDirectory.unwrap(dir))).getDirectory().toString(),
                String.format("%s.hnsw", segmentName)).toString();

        AccessController.doPrivileged(
                new PrivilegedAction<Void>() {
                    public Void run() {
                        KNNIndex.saveIndex(docs, vectors, indexPath);
                        return null;
                    }
                }
        );

        assertTrue(Arrays.asList(dir.listAll()).contains("_dummy.hnsw"));
        dir.close();
    }

    public void testQueryHnswIndex() throws Exception {
        int[] docs = {0, 1, 2};

        float[][] vectors = {
                {1.0f, 2.0f, 3.0f, 4.0f},
                {5.0f, 6.0f, 7.0f, 8.0f},
                {9.0f, 10.0f, 11.0f, 12.0f}
        };

        Directory dir = newFSDirectory(createTempDir());
        String segmentName = "_dummy1";
        String indexPath = Paths.get(((FSDirectory) (FilterDirectory.unwrap(dir))).getDirectory().toString(),
                String.format("%s.hnsw", segmentName)).toString();

        AccessController.doPrivileged(
                new PrivilegedAction<Void>() {
                    public Void run() {
                        KNNIndex.saveIndex(docs, vectors, indexPath);
                        return null;
                    }
                }
        );

        assertTrue(Arrays.asList(dir.listAll()).contains("_dummy1.hnsw"));

        float[] queryVector = {1.0f, 1.0f, 1.0f, 1.0f};

        KNNQueryResult[] results = AccessController.doPrivileged(
                new PrivilegedAction<KNNQueryResult[]>() {
                    public KNNQueryResult[] run() {
                        KNNIndex index = KNNIndex.loadIndex(indexPath.toString());
                        logger.info(index.getIndex());
                        return index.queryIndex(queryVector, 30);
                    }
                }
        );

        Map<Integer, Float> scores = Arrays.stream(results).collect(
                Collectors.toMap(result -> result.getId(), result -> result.getScore()));
        logger.info("PRINTING...MAP.......");
        logger.info(scores);

        assertEquals(results.length, 3);
        logger.info("VAMSHI>>>>>>>>>>>>>>>>>>>>>>");
        logger.info(results[0].getScore() + ":" + results[0].getId());
        logger.info(results[1].getScore() + ": "+ results[1].getId());
        logger.info(results[2].getScore() + ": "+ results[2].getId());
        assertEquals(results[0].getId(), 0);
        assertEquals(results[1].getId(), 1);
        fail("VAMOIII");
        dir.close();

    }
}