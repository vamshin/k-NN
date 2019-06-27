/*
 *   Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
 */

package org.elasticsearch.index.knn;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.util.DocIdSetBuilder;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.index.knn.codec.KNNCodec;
import org.elasticsearch.index.knn.v1736.KNNIndex;

import java.io.IOException;
import java.nio.file.Path;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Calculate query weights and build query scorers.
 */
public class KNNWeight extends Weight {
    private static Logger logger = LogManager.getLogger(KNNWeight.class);
    private final KNNQuery knnQuery;
    private final float boost;

    public static KNNIndexCache knnIndexCache = new KNNIndexCache();

    public KNNWeight(KNNQuery query, float boost) {
        super(query);
        this.knnQuery = query;
        this.boost = boost;
    }

    @Override
    public Explanation explain(LeafReaderContext context, int doc) {
        return Explanation.match(1.0f, "No Explanation");
    }

    @Override
    public void extractTerms(Set<Term> terms) {
    }

    @Override
    public Scorer scorer(LeafReaderContext context) throws IOException {
        try {
            SegmentReader reader = (SegmentReader) FilterLeafReader.unwrap(context.reader());
            String directory = ((FSDirectory) FilterDirectory.unwrap(reader.directory())).getDirectory().toString();

            /**
             * In case of compound file, extension would be .hnswc otherwise .hnsw
             */
            String hnswFileExtension = reader.getSegmentInfo().info.getUseCompoundFile()
                                               ? KNNCodec.HNSW_COMPUND_EXTENSION : KNNCodec.HNSW_EXTENSION;
            List<String> hnswFiles = reader.getSegmentInfo().files().stream().filter(fileName -> fileName.endsWith(hnswFileExtension))
                                          .collect(Collectors.toList());

            // Able to proceed further if the corresponding hnsw index does not present
            if(hnswFiles.size() != 1) {
                throw new IllegalStateException("More than one hnsw extension for the segment: "
                                                        + reader.getSegmentName());
            }

            /**
             * Add logic to pick up the right nmslib version based on the version
             * in the name of the file. As of now we have one version 1.7.3.6.
             * So defering this to future release
             */

            Path indexPath = PathUtils.get(directory, hnswFiles.get(0));
            KNNQueryResult[] results = AccessController.doPrivileged(
                    new PrivilegedAction<KNNQueryResult[]>() {
                        public KNNQueryResult[] run() {
                            KNNIndex index = knnIndexCache.getIndex(indexPath.toString());
                            if(index.isDeleted.get()) {
                                // Race condition occured. Looks like entry got evicted from cache and
                                // possibly gc. Try to read again
                                logger.info("[KNN] Race condition occured. Looks like entry got evicted " +
                                                    "from cache and possible gc. Trying to read again");
                                index = knnIndexCache.getIndex(indexPath.toString());
                            }
                            return index != null ? index.queryIndex(knnQuery.getQueryVector(), knnQuery.getK()) : null;
                        }
                    }
            );

            Map<Integer, Float> scores = Collections.emptyMap();
            if (results != null) {
                scores = Arrays.stream(results).collect(
                        Collectors.toMap(result -> result.getId(), result -> result.getScore()));
            }

            int maxDoc = Collections.max(scores.keySet()) + 1;
            DocIdSetBuilder docIdSetBuilder = new DocIdSetBuilder(maxDoc);
            DocIdSetBuilder.BulkAdder setAdder = docIdSetBuilder.grow(maxDoc);
            if(results != null)
                Arrays.stream(results).forEach(result -> setAdder.add(result.getId()));
            DocIdSetIterator docIdSetIter = docIdSetBuilder.build().iterator();
            return new KNNScorer(this, docIdSetIter, scores, boost);
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    public boolean isCacheable(LeafReaderContext context) {
        return true;
    }
}
