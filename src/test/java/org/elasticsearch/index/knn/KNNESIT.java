package org.elasticsearch.index.knn;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;

@ESIntegTestCase.ClusterScope(scope=ESIntegTestCase.Scope.SUITE, numDataNodes=1)
public class KNNESIT extends ESIntegTestCase {
    @Override
    public Settings indexSettings() {
        return Settings.builder()
                       .put(super.indexSettings())

                       // .put("index.location_override", "poc/data/nodes/0/indices/-0Z8d0vZQdWEjLMknPvzqQ/0/index/")
                       .put("number_of_shards", 1)
                       .put("number_of_replicas", 0)
                       .build();
    }

    /**
     * Able to add docs to KNN index
     */
    public void testAddKNNDoc() {

    }

    /**
     * Able to update docs in KNN index
     */
    public void testUpdateKNNDoc() {

    }

    /**
     * Able to delete docs in KNN index
     */
    public void testDeleteKNNDoc() {

    }
}