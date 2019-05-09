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

import java.nio.file.Path;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Calculate query weights and build query scorers.
 */
public class KNNWeight extends Weight {

    private final KNNQuery knnQuery;
    private final float boost;

    public KNNWeight(KNNQuery query, float boost) {
        super(query);
        this.knnQuery = query;
        this.boost = boost;
    }

    @Override
    public Explanation explain(LeafReaderContext context, int doc) {
        return Explanation.match(101, "No explanation");
    }

    @Override
    public void extractTerms(Set<Term> terms) {
    }

    @Override
    public Scorer scorer(LeafReaderContext context) {
        try {
            SegmentReader reader = (SegmentReader) FilterLeafReader.unwrap(context.reader());
            String directory = ((FSDirectory) FilterDirectory.unwrap(reader.directory())).getDirectory().toString();
            Path indexPath = PathUtils.get(directory, String.format("%s.hnsw", reader.getSegmentName()));

            KNNQueryResult[] results = AccessController.doPrivileged(
                    new PrivilegedAction<KNNQueryResult[]>() {
                        public KNNQueryResult[] run() {
                            KNNIndex index = KNNIndex.loadIndex(indexPath.toString());
                            return index.queryIndex(knnQuery.getQueryVector(), knnQuery.getK());
                        }
                    }
            );

            Map<Integer, Float> scores = Arrays.stream(results).collect(
                    Collectors.toMap(result -> result.getId(), result -> result.getScore()));

            int maxDoc = Collections.max(scores.keySet()) + 1;
            DocIdSetBuilder docIdSetBuilder = new DocIdSetBuilder(maxDoc);
            DocIdSetBuilder.BulkAdder setAdder = docIdSetBuilder.grow(maxDoc);

            Arrays.stream(results).forEach(result -> setAdder.add(result.getId()));

            DocIdSetIterator docIdSetIter = docIdSetBuilder.build().iterator();
            return new KNNScorer(this, docIdSetIter, scores, boost);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean isCacheable(LeafReaderContext context) {
        return true;
    }
}
