package org.elasticsearch.index.knn;


import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.DocumentMapperParser;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.indices.mapper.MapperRegistry;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.junit.Before;

import java.util.Collection;
import java.util.Collections;
import java.util.function.Supplier;

public class KNNMapperTests extends ESSingleNodeTestCase {

    MapperRegistry mapperRegistry;
    IndexService indexService;
    DocumentMapperParser parser;

    @Before
    public void setup() {
        indexService = createIndex("test");
        indexService.getIndexSettings().getIndex();
        mapperRegistry = new MapperRegistry(
            Collections.singletonMap(KNNVectorFieldMapper.CONTENT_TYPE, new KNNVectorFieldMapper.TypeParser()),
            Collections.emptyMap(), MapperPlugin.NOOP_FIELD_FILTER);
        Supplier<QueryShardContext> queryShardContext = () -> {
            return indexService.newQueryShardContext(0, null, () -> { throw new UnsupportedOperationException(); }, null);
        };
        parser = new DocumentMapperParser(indexService.getIndexSettings(), indexService.mapperService(), indexService.getIndexAnalyzers(),
            indexService.xContentRegistry(), indexService.similarityService(), mapperRegistry, queryShardContext);
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(InternalSettingsPlugin.class);
    }

    public void testDefaults() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties").startObject("field")
            .field("type", "knn_vector")
            .endObject().endObject().endObject().endObject());
        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));
//        ParsedDocument parsedDoc = mapper.parse(SourceToParse.source("test", "type", "1", BytesReference.bytes(XContentFactory.jsonBuilder()
//                .startObject()
//                .startArray("field")
//                .value(1.0f)
//                .value(2.0f)
//                .endArray()
//                .endObject()),
//            XContentType.JSON));
//        IndexableField[] fields = parsedDoc.rootDoc().getFields("field");
//        assertNotNull(fields);
//        assertEquals(Arrays.toString(fields), 1, fields.length);
//        IndexableField field = fields[0];
//        assertEquals(IndexOptions.NONE, field.fieldType().indexOptions());
    }
}
