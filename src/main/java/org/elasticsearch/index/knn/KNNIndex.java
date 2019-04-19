package org.elasticsearch.index.knn;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.ByteArrayDataInput;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

//import lombok.experimental.Delegate;
//import lombok.RequiredArgsConstructor;
//import lombok.NonNull;
//import lombok.SneakyThrows;
//import lombok.Data;
//import lombok.EqualsAndHashCode;
//import lombok.Getter;

//@Data
public class KNNIndex {

    static {
        System.loadLibrary("KNNIndex");
    }

    private long index;

    // All pointers to C heap.

    public long getIndex() {
        return index;
    }

    public void setIndex(long index) {
        this.index = index;
    }

    private static Map<String, KNNIndex> loadedIndices = new ConcurrentHashMap<>();

    public static native void saveIndex(int[] ids, float[][] data, String indexPath);

    public native KNNQueryResult[] queryIndex(float[] query, int k);

    public static KNNIndex loadIndex(String indexPath) {
        KNNIndex loadedIndex = loadedIndices.get(indexPath);
        if (loadedIndex == null) {
            KNNIndex index = new KNNIndex();
            index.init(indexPath);
            loadedIndices.put(indexPath, index);
            loadedIndex = index;
        }
        return loadedIndex;
    }

    //@SneakyThrows
    public static KNNIndex loadIndex2(BinaryDocValues values, String indexPath) {
        try {
            KNNIndex loadedIndex = loadedIndices.get(indexPath);
            if (loadedIndex == null) {
                ArrayList<float[]> vectorList = new ArrayList<>();
                for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
                    byte[] value = values.binaryValue().bytes;
                    ByteArrayDataInput input = new ByteArrayDataInput(value);
                    int size = input.readVInt();
                    assert size == 1;
                    int length = input.readVInt();
                    byte[] vec_value = new byte[length];
                    input.readBytes(vec_value, 0, length);
                    try (ByteArrayInputStream byteStream = new ByteArrayInputStream(vec_value);
                         ObjectInputStream objectStream = new ObjectInputStream(byteStream);) {
                        float[] vector = (float[])objectStream.readObject();
                        vectorList.add(vector);
                    }
                }
                float[][] vectors = vectorList.toArray(new float[][] {});

                KNNIndex index = new KNNIndex();
                index.init2(vectors, indexPath);

                loadedIndices.put(indexPath, index);
                loadedIndex = index;
            }
            return loadedIndex;
        } catch(Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static KNNIndex loadIndex3(float[][] vectors, String indexPath) {
        KNNIndex loadedIndex = loadedIndices.get(indexPath);
        if (loadedIndex == null) {
            KNNIndex index = new KNNIndex();
            index.init2(vectors, indexPath);
            loadedIndices.put(indexPath, index);
            loadedIndex = index;
        }
        return loadedIndex;
    }

    public native void init2(float[][] data, String indexPath);

    public native void init(String indexPath);

    public native void gc();
}
