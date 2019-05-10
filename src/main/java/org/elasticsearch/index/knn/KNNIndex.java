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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * JNI layer to communicate with the nmslib
 */
public class KNNIndex {
    private static final Logger logger = LogManager.getLogger(KNNIndex.class);

    static {
        System.loadLibrary("KNNIndex");
    }

    private long index;

    public long getIndex() {
        return index;
    }

    public void setIndex(long index) {
        this.index = index;
    }

    private static Map<String, KNNIndex> loadedIndices = new ConcurrentHashMap<>();

    public static native void saveIndex(int[] ids, float[][] data, String indexPath);

    public native KNNQueryResult[] queryIndex(float[] query, int k);

    /**
     * Loads the knn index to memory for querying the neighbours
     *
     * @param indexPath path where the hnsw index is stored
     * @return knn index that can be querried for k nearest neighbours
     */
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

    public native void init(String indexPath);

    public native void gc();
}
