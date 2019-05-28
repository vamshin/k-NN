package org.elasticsearch.index.knn;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;

@ESIntegTestCase.ClusterScope(scope=ESIntegTestCase.Scope.SUITE, numDataNodes=1)
public class KNNSearcherIT extends ESIntegTestCase {
    private static final Logger logger = LogManager.getLogger(KNNMapperIT.class);

    @Override
    public Settings indexSettings() {
        return Settings.builder()
                       .put(super.indexSettings())
                       .put("number_of_shards", 1)
                       .put("number_of_replicas", 0)
                       .put("index.codec", "KNNCodec")
                       .build();
    }

    private void createKnnIndex(String index) {
        createIndex(index, indexSettings());
        PutMappingRequest request = new PutMappingRequest(index).type("_doc");

        request.source(
                "{\n" +
                        "  \"properties\": {\n" +
                        "    \"my_vector\": {\n" +
                        "      \"type\": \"knn_vector\"\n" +
                        "    }\n" +
                        "  }\n" +
                        "}",
                XContentType.JSON);
        ElasticsearchAssertions.assertAcked(client().admin().indices().putMapping(request).actionGet());
    }

    /**
     * For invalid K, query builder should throw Exception
     */
    public void testInvalidK() {
        float[] vector = {1.0f, 2.0f, 3.0f};
        expectThrows(IllegalArgumentException.class, () -> new KNNQueryBuilder("myvector", vector, -1));
    }

    /**
     * For valid K, Search should be able to give results
     */
    public void testValidK() {


    }

    /**
     * K  number of docs
     */
    public void testLargeK() {

    }


    /**
     * K &lt; &lt; number of docs
     */
    public void testSmallerK() {

    }

    /**
     * post filter logic test with knn
     */
    public void testPostFilterWithK() {

    }

    /**
     * pre filter logic test with knn
     */
    public void testPreFilterWithK() {

    }
}

