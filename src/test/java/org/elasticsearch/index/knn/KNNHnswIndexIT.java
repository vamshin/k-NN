package org.elasticsearch.index.knn;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FilterDirectory;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.index.knn.codec.KNNCodec;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.watcher.ResourceWatcherService;

import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;


@ESIntegTestCase.ClusterScope(scope=ESIntegTestCase.Scope.SUITE, numDataNodes=1)
public class KNNHnswIndexIT extends ESIntegTestCase {

    public void testPoints() throws Exception {

//        Settings nodeSettings = Settings.builder()
//                                        .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir())
//                                        .build();
//        IndexSettings settings = IndexSettingsModule.newIndexSettings("_na", nodeSettings);
//        SimilarityService similarityService = new SimilarityService(settings, null, Collections.emptyMap());
//        IndexAnalyzers indexAnalyzers = createTestAnalysis(settings, nodeSettings).indexAnalyzers;
//        MapperRegistry mapperRegistry = new MapperRegistry(Collections.emptyMap(), Collections.emptyMap(), MapperPlugin.NOOP_FIELD_FILTER);
//        MapperService mapperService = new MapperService(settings, indexAnalyzers, xContentRegistry(), similarityService, mapperRegistry,
//                () -> null);

        Directory dir = newFSDirectory(createTempDir());
        IndexWriterConfig iwc = newIndexWriterConfig();
        // Else seeds may not reproduce:
        iwc.setMergeScheduler(new SerialMergeScheduler());
        iwc.setCodec(new KNNCodec()); //mapperService, logger));

        float[] array = {1.0f, 2.0f, 3.0f};
        float[] array1 = {2.0f, 3.0f, 4.0f};


        VectorField vectorField = new VectorField("test_vector", array, KNNVectorFieldMapper.Defaults.FIELD_TYPE);
        VectorField vectorField1 = new VectorField("test_vector", array1, KNNVectorFieldMapper.Defaults.FIELD_TYPE);


        RandomIndexWriter writer = new RandomIndexWriter(random(), dir, iwc);
//        for (int i = 0; i < 2; i++) {
            Document doc = new Document();
//            doc.add(new FloatPoint("field", -3.257986755371093750e+02f,-2.752000122070312500e+02f,-7.057591552734375000e+02f,-7.937044067382812500e+02f,6.565161132812500000e+02f,9.952614746093750000e+02f,-3.282644653320312500e+02f,2.603036193847656250e+02f));
//            doc.add(new FloatPoint("field2", 3.0f, 5.0f));

            doc.add(vectorField);

            writer.addDocument(doc);
            doc = new Document();
             doc.add(vectorField1);
            writer.addDocument(doc);

//        }

        KNNIndexCache.setKnnIndexFileListener(new MockKNNIndexFileListener(null));
        IndexReader reader = writer.getReader();


        LeafReaderContext lrc = reader.getContext().leaves().iterator().next(); // leaf reader context
        SegmentReader segmentReader = (SegmentReader) FilterLeafReader.unwrap(lrc.reader());
        String directory = ((FSDirectory) FilterDirectory.unwrap(segmentReader.directory())).getDirectory().toString();
        String hnswFileExtension = segmentReader.getSegmentInfo().info.getUseCompoundFile()
                                           ? KNNCodec.HNSW_COMPUND_EXTENSION : KNNCodec.HNSW_EXTENSION;
        String hnswSuffix = "test_vector" + hnswFileExtension;
        List<String> hnswFiles = segmentReader.getSegmentInfo().files().stream()
                                       .filter(fileName -> fileName.endsWith(hnswSuffix))
                                       .collect(Collectors.toList());
        assertTrue(!hnswFiles.isEmpty());
        Path indexPath = PathUtils.get(directory, hnswFiles.get(0));


        IndexSearcher searcher = newSearcher(reader);
        assertEquals(1, searcher.count(new KNNQuery("test_vector", new float[] {1.0f, 2.5f}, 1)));

        reader.close();
        writer.close();
        dir.close();
    }




}


class MockKNNIndexFileListener extends KNNIndexFileListener {

    public MockKNNIndexFileListener(ResourceWatcherService resourceWatcherService) {
        super(resourceWatcherService);
    }

    public void register(Path filePath) throws Exception {
    }

    @Override
    public void onFileDeleted(Path indexFilePath) {

    }
}