package org.elasticsearch.index.knn;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.*;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.util.DocIdSetBuilder;
import org.elasticsearch.SpecialPermission;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class KNNWeight extends Weight {

    private final KNNQuery knnQuery;
    private final Logger logger;

    public KNNWeight(KNNQuery query, Logger logger) {
        super(query);
        this.knnQuery = query;
        this.logger = logger;
    }

    @Override
    public Explanation explain(LeafReaderContext context, int doc) {
        return Explanation.match(101, "No explanation");
    }

    @Override
    public void extractTerms(Set<Term> terms) {
    }

    @Override
    //@SneakyThrows
    public Scorer scorer(LeafReaderContext context) {
        try {
            SegmentReader reader = (SegmentReader) FilterLeafReader.unwrap(context.reader());
            String directory = ((FSDirectory) FilterDirectory.unwrap(reader.directory())).getDirectory().toString();
            Path indexPath = Paths.get(directory, String.format("%s.hnsw", reader.getSegmentName()));

            //values needs to be passed to NMS due to a bug
            BinaryDocValues values = context.reader().getBinaryDocValues(knnQuery.getField());
           // KNNCodec.Pair vectors = KNNCodec.getFloats(values);
           // KNNIndex index = KNNIndex.loadIndex3(vectors.vectors, indexPath.toString());
            SecurityManager sm = System.getSecurityManager();
            if (sm != null) {
                // unprivileged code such as scripts do not have SpecialPermission
                sm.checkPermission(new SpecialPermission());
            }

            KNNQueryResult[] results = AccessController.doPrivileged(
                new PrivilegedAction<KNNQueryResult[]>() {
                    public KNNQueryResult[] run() {
                        KNNIndex index = KNNIndex.loadIndex(indexPath.toString());
                        return index.queryIndex(knnQuery.getQueryVector(), knnQuery.getK());
                    }
                }
            );

            Map<Integer, Float> scores = new HashMap<>();
            for (int i = 0; i < results.length; i++) {
                KNNQueryResult result = results[i];
                scores.put((Integer)result.getId(), result.getScore());
            }

            int maxDoc = Collections.max(scores.keySet()) + 1;
            DocIdSetBuilder docIdSetBuilder = new DocIdSetBuilder(maxDoc);
            DocIdSetBuilder.BulkAdder setAdder = docIdSetBuilder.grow(maxDoc);
            for (int i = 0; i < results.length; i++) {
                setAdder.add(results[i].getId());
                //logger.warn("DocID {}", results[i].getId());
            }
            DocIdSetIterator docIdSetIter = docIdSetBuilder.build().iterator();
            return new KNNScorer(docIdSetIter, scores);
        } catch(Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean isCacheable(LeafReaderContext context) {
        return true;
    }
}
