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

package org.elasticsearch.index.knn.v1736;

import org.elasticsearch.index.knn.KNNQueryResult;
import org.elasticsearch.index.knn.util.NmsLibVersion;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * JNI layer to communicate with the nmslib
 * This class refers to the nms library build with version tag 1.7.3.6
 * See <a href="https://github.com/nmslib/nmslib/tree/v1.7.3.6">tag1.7.3.6</a>
 */
public class KNNIndex {

    public static NmsLibVersion VERSION = NmsLibVersion.V1736;

    static {
        System.loadLibrary(NmsLibVersion.V1736.indexLibraryVersion());
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
     * @return knn index that can be queried for k nearest neighbours
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
