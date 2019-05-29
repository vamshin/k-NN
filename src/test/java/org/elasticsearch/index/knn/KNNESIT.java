package org.elasticsearch.index.knn;

import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;

import java.io.IOException;

@ESIntegTestCase.ClusterScope(scope=ESIntegTestCase.Scope.SUITE, numDataNodes=1)
public class KNNESIT extends ESIntegTestCase {

    @Override
    public Settings indexSettings() {
        return Settings.builder()
                       .put(super.indexSettings())
                       .put("number_of_shards", 1)
                       .put("number_of_replicas", 0)
                       .put("index.codec", "KNNCodec")
                       .build();
    }

    private void addKnnDoc(String index, String docId, Object[] vector) throws IOException {
        IndexResponse response = client().prepareIndex(index, "_doc", docId)
                                         .setSource(XContentFactory.jsonBuilder()
                                                                   .startObject()
                                                                   .array("my_vector", vector)
                                                                   .field("price", 10)
                                                                   .endObject())
                                         .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                                         .get();
        if(!response.status().equals(RestStatus.OK) && !response.status().equals(RestStatus.CREATED)) {
            fail("Bad response while adding doc");
        }
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
     * Able to add docs to KNN index
     */
    public void testAddKNNDoc() throws Exception {
        createKnnIndex("testindex");
        Float[] vector  = {6.0f, 6.0f};
        addKnnDoc("testindex", "1", vector);
    }

    /**
     * Able to update docs in KNN index
     */
    public void testUpdateKNNDoc() throws Exception {
        createKnnIndex("testindex");
        Float[] vector  = {6.0f, 6.0f};
        addKnnDoc("testindex", "1", vector);

        // update
        Float[] updatedVector  = {8.0f, 8.0f};
        addKnnDoc("testindex", "1", vector);
    }

    /**
     * Able to delete docs in KNN index
     */
    public void testDeleteKNNDoc() throws Exception {
        createKnnIndex("testindex");
        Float[] vector  = {6.0f, 6.0f};
        addKnnDoc("testindex", "1", vector);

        // delete knn doc
        DeleteResponse response = client().prepareDelete("testindex", "_doc", "1")
                                          .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                                          .get();
        assertEquals(RestStatus.OK, response.status());
    }
}