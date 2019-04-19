package org.elasticsearch.index.knn;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Weight;

//import lombok.experimental.Delegate;
//import lombok.RequiredArgsConstructor;
//import lombok.NonNull;
//import lombok.SneakyThrows;
//import lombok.Data;
//import lombok.EqualsAndHashCode;
//import lombok.Getter;


//@RequiredArgsConstructor
//@Getter
//@EqualsAndHashCode(callSuper=false)
public class KNNQuery extends Query {

    private String field;
    private float[] queryVector;
    private int k;
    private Logger logger;

    public KNNQuery(String field, float[] queryVector, int k, Logger logger) {
        this.field = field;
        this.queryVector = queryVector;
        this.k =  k;
        this.logger = logger;
    }


    public String getField() {
        return this.field;
    }

    public float[] getQueryVector() {
        return this.queryVector;
    }

    public int getK() {
        return this.k;
    }

    @Override
    //@SneakyThrows
    public Weight createWeight(IndexSearcher searcher, boolean needsScores, float boost) {
        //System.out.println("Total docs: " + searcher.getIndexReader().maxDoc());
        //System.out.println("Total segs: " + searcher.getIndexReader().leaves().size());
        return new KNNWeight(this, this.logger);
    }

    @Override
    public String toString(String field) {
        return field;
    }

    @Override
    public int hashCode() {
        return field.hashCode() ^ queryVector.hashCode() ^ k;
    }

    @Override
    public boolean equals(Object other) {
        return sameClassAs(other) &&
            equalsTo(getClass().cast(other));
    }

    private boolean equalsTo(KNNQuery other) {
        return this.field.equals(other.getField()) && this.queryVector.equals(other.getQueryVector()) && this.k == other.getK();
    }
};
