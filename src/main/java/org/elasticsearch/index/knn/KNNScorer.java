package org.elasticsearch.index.knn;

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Scorer;

import java.util.Map;

public class KNNScorer extends Scorer {

    private final DocIdSetIterator docIdsIter;
    private final Map<Integer, Float> scores;

    public KNNScorer(DocIdSetIterator docIdsIter, Map<Integer, Float> scores) {
        super(null);
        this.docIdsIter = docIdsIter;
        this.scores = scores;
    }

    @Override
    public DocIdSetIterator iterator() {
        return docIdsIter;
    }

    @Override
    public float score() {
        Float score = scores.get(docID());
        if (score == null) {
            throw new RuntimeException("Not expected");
        } else {
            return score;
        }
    }

    @Override
    public int docID() {
        return docIdsIter.docID();
    }
}
