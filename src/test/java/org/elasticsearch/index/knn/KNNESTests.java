package org.elasticsearch.index.knn;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;

import static org.hamcrest.core.Is.is;

@ESIntegTestCase.ClusterScope(scope=ESIntegTestCase.Scope.SUITE, numDataNodes=1)
public class KNNESTests extends ESIntegTestCase {
    @Override
    public Settings indexSettings() {
        return Settings.builder()
            .put(super.indexSettings())

            // .put("index.location_override", "poc/data/nodes/0/indices/-0Z8d0vZQdWEjLMknPvzqQ/0/index/")
            .put("number_of_shards", 1)
            .put("number_of_replicas", 0)
            .build();
    }

    public void testKNN() {
        // Create an index and index some documents
        String indexName = "test";// + randomIntBetween(1, 1000);
        createIndex(indexName);
        long nbDocs = 1;//randomIntBetween(10, 1000);
        for (long i = 0; i < nbDocs; i++) {
            index(indexName, "knn_doc", "" + i, "foo", new float[]{1.0f, 2.0f});
        }
        flushAndRefresh();
        logger.info("\n\n+==========+\n\n");
        SearchResponse response = client().prepareSearch(indexName)
            .setSearchType(SearchType.QUERY_THEN_FETCH)
            .setQuery(new KNNQueryBuilder("foo", new float[] {1.0f, 2.0f}, 1)).get();
        assertThat(response.getHits().getTotalHits(), is(nbDocs));
    }
}
