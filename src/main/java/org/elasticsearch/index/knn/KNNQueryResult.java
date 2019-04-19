package org.elasticsearch.index.knn;

//import lombok.experimental.Delegate;
//import lombok.RequiredArgsConstructor;
//import lombok.NonNull;
//import lombok.SneakyThrows;
//import lombok.Data;
//import lombok.EqualsAndHashCode;
//import lombok.Getter;


//@Data
public class KNNQueryResult
{
    public KNNQueryResult(int id, float score) {
        this.id = id;
        this.score = score;
    }

    private int id;
    private float score;

    public int getId() {
        return this.id;
    }

    public float getScore() {
        return this.score;
    }
}
