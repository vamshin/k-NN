/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.knn;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.knn.codec.KNNCodec;
import org.elasticsearch.test.ESIntegTestCase;

import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedAction;

@ESIntegTestCase.ClusterScope(scope=ESIntegTestCase.Scope.SUITE, numDataNodes=1)
public class KNNCodecIT extends ESIntegTestCase {
    private static final Logger logger = LogManager.getLogger(KNNCodecIT.class);

//    @BeforeClass
//    public void setUp() {
//        Codec.setDefault(new Lucene80Codec());
//    }
//    @BeforeClass
//public void setUp() throws Exception {
//    super.setUp();
//    // set the default codec, so adding test cases to this isn't fragile
//     Codec.getDefault();
////    Codec.setDefault(getCodec());
//}
//
//    public void tearDown() throws Exception {
////        Codec.setDefault(savedCodec); // restore
//        super.tearDown();
//    }

    @Override
    public Settings indexSettings() {
        return Settings.builder()
                       .put(super.indexSettings())

                       // .put("index.location_override", "poc/data/nodes/0/indices/-0Z8d0vZQdWEjLMknPvzqQ/0/index/")
                       .put("number_of_shards", 1)
                       .put("number_of_replicas", 0)
                       .put("index.codec", "KNNCodec")
                       .build();
    }


    public void testPoints() throws Exception {

//        Settings nodeSettings = Settings.builder()
//            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir())
//            .build();
//        IndexSettings settings = IndexSettingsModule.newIndexSettings("_na", nodeSettings);
//        SimilarityService similarityService = new SimilarityService(settings, null, Collections.emptyMap());
//        IndexAnalyzers indexAnalyzers = createTestAnalysis(settings, nodeSettings).indexAnalyzers;
//        MapperRegistry mapperRegistry = new MapperRegistry(Collections.emptyMap(), Collections.emptyMap(), MapperPlugin.NOOP_FIELD_FILTER);
//        MapperService mapperService = new MapperService(settings, indexAnalyzers, xContentRegistry(), similarityService, mapperRegistry,
//            () -> null);

        String mappings = "\"properties\":{\"field-1\":{\"type\":\"integer\"}}";
        createIndex("testIndex", indexSettings());
//        PutMappingRequestBuilder builder = PutMappingRequestBuilder.
        CreateIndexRequest cr = new CreateIndexRequest();




        Directory dir = newFSDirectory(createTempDir());
        IndexWriterConfig iwc = newIndexWriterConfig();
         // Else seeds may not reproduce:
        iwc.setMergeScheduler(new SerialMergeScheduler());
        logger.info("VASMDADSFASFADASFADSDSFDSAFASDAf.......#############################");
        iwc.setCodec(new KNNCodec()); //mapperService, logger));

        RandomIndexWriter writer = new RandomIndexWriter(random(), dir, iwc);
        for (int i = 0; i < 2; i++) {
            Document doc = new Document();
            doc.add(new FloatPoint("field", -3.257986755371093750e+02f,-2.752000122070312500e+02f,-7.057591552734375000e+02f,-7.937044067382812500e+02f,6.565161132812500000e+02f,9.952614746093750000e+02f,-3.282644653320312500e+02f,2.603036193847656250e+02f));
            doc.add(new FloatPoint("field2", 3.0f, 5.0f));

            writer.addDocument(doc);
        }
        //writer.re
        //move this to Query tests later

        writer.flush();
        IndexReader reader = writer.getReader();
        IndexSearcher searcher = newSearcher(reader);




        int count1;

        int counter = AccessController.doPrivileged(
                new PrivilegedAction<Integer>() {
                    public Integer run() {

                        try {
                            return searcher.count(new KNNQuery("field", new float[] {1.0f, 2.5f}, 1));
                        } catch (IOException e) {
                            logger.info(e);
                        }
                        return 100000;
                    }
                }
        );



        assertEquals(1, counter);
//        assertEquals(1, searcher.count(new KNNQuery("field", new float[] {1.0f, 2.5f}, 1, null)));

        reader.close();
        writer.close();
        dir.close();
    }
}
