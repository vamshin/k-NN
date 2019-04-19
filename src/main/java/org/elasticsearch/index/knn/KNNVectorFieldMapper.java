package org.elasticsearch.index.knn;

import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.mapper.*;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.QueryShardException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.mapper.TypeParsers.parseField;

/**
 * Field Mapper for KNN vector types.
 *
 */
public class KNNVectorFieldMapper extends FieldMapper implements ArrayValueMapperParser {
    public static final String CONTENT_TYPE = "knn_vector";

    public static class Names {
        public static final String IGNORE_MALFORMED = "ignore_malformed";
    }

    public static class Defaults {
        public static final Explicit<Boolean> IGNORE_MALFORMED = new Explicit<>(false, false);
        public static final KNNVectorFieldType FIELD_TYPE = new KNNVectorFieldType();

        static {
            FIELD_TYPE.setTokenized(false);
            FIELD_TYPE.setIndexOptions(IndexOptions.NONE);
            FIELD_TYPE.setHasDocValues(true);
            FIELD_TYPE.setDocValuesType(DocValuesType.BINARY);
            FIELD_TYPE.freeze();
        }
    }

    public static class Builder extends FieldMapper.Builder<Builder, KNNVectorFieldMapper> {
        protected Boolean ignoreMalformed;

        public Builder(String name) {
            super(name, Defaults.FIELD_TYPE, Defaults.FIELD_TYPE);
            builder = this;
        }

        public Builder ignoreMalformed(boolean ignoreMalformed) {
            this.ignoreMalformed = ignoreMalformed;
            return builder;
        }

        protected Explicit<Boolean> ignoreMalformed(BuilderContext context) {
            if (ignoreMalformed != null) {
                return new Explicit<>(ignoreMalformed, true);
            }
            if (context.indexSettings() != null) {
                return new Explicit<>(IGNORE_MALFORMED_SETTING.get(context.indexSettings()), false);
            }
            return KNNVectorFieldMapper.Defaults.IGNORE_MALFORMED;
        }


        public KNNVectorFieldMapper build(BuilderContext context, String simpleName, MappedFieldType fieldType,
                                          MappedFieldType defaultFieldType, Settings indexSettings,
                                          MultiFields multiFields, Explicit<Boolean> ignoreMalformed,
                                          CopyTo copyTo) {
            setupFieldType(context);
            return new KNNVectorFieldMapper(simpleName, fieldType, defaultFieldType, indexSettings, multiFields,
                ignoreMalformed, copyTo);
        }

        @Override
        public KNNVectorFieldMapper build(BuilderContext context) {
            return build(context, name, fieldType, defaultFieldType, context.indexSettings(),
                multiFieldsBuilder.build(this, context), ignoreMalformed(context), copyTo);
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder<?,?> parse(String name, Map<String, Object> node, ParserContext parserContext)
            throws MapperParsingException {
            Builder builder = new KNNVectorFieldMapper.Builder(name);
            parseField(builder, name, node, parserContext);
            for (Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator(); iterator.hasNext();) {
                Map.Entry<String, Object> entry = iterator.next();
                String propName = entry.getKey();
                Object propNode = entry.getValue();

                if (propName.equals(Names.IGNORE_MALFORMED)) {
                    builder.ignoreMalformed(XContentMapValues.nodeBooleanValue(propNode, name + "." + Names.IGNORE_MALFORMED));
                    iterator.remove();
                }
            }
            return builder;
        }
    }

    protected Explicit<Boolean> ignoreMalformed;

    public KNNVectorFieldMapper(String simpleName, MappedFieldType fieldType, MappedFieldType defaultFieldType,
                                Settings indexSettings, MultiFields multiFields, Explicit<Boolean> ignoreMalformed,
                                CopyTo copyTo) {
        super(simpleName, fieldType, defaultFieldType, indexSettings, multiFields, copyTo);
        this.ignoreMalformed = ignoreMalformed;
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    protected void parseCreateField(ParseContext context, List<IndexableField> fields) throws IOException {
        throw new UnsupportedOperationException("Parsing is implemented in parse(), this method should NEVER be called");
    }

    public static class KNNVectorFieldType extends MappedFieldType {
        public KNNVectorFieldType() {
        }

        KNNVectorFieldType(KNNVectorFieldType ref) {
            super(ref);
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public MappedFieldType clone() {
            return new KNNVectorFieldType(this);
        }

        @Override
        public Query existsQuery(QueryShardContext context) {
            return new TermQuery(new Term(FieldNamesFieldMapper.NAME, name()));
        }

        @Override
        public Query termQuery(Object value, QueryShardContext context) {
            throw new QueryShardException(context, "KNN vector do not support exact searching, use KNN queries instead: ["
                + name() + "]");
        }
    }

    @Override
    public void parse(ParseContext context) throws IOException {
        context.path().add(simpleName());
        VectorField point;
        Float f = context.parseExternalValue(Float.class);

        if (f == null) {
            ArrayList<Float> vector = new ArrayList<>();
            XContentParser.Token token = context.parser().currentToken();

            if (token == XContentParser.Token.START_ARRAY) {
                token = context.parser().nextToken();
                while (token != XContentParser.Token.END_ARRAY) {
                    vector.add(context.parser().floatValue());
                    token = context.parser().nextToken();
                }
            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                vector.add(context.parser().floatValue());
                token = context.parser().nextToken();
            }
            float[] array = new float[vector.size()];
            for (int i = 0; i < vector.size(); i++) {
                array[i] = vector.get(i);
            }
            point = new VectorField(name(), array, fieldType());
        } else {
            float[] array = {f};
            point = new VectorField(name(), array, fieldType());
        }

        context.doc().add(point);
        if (fieldType().stored()) {
            context.doc().add(new StoredField(name(), point.toString()));
        }
        context.path().remove();
       // return null;
    }

    @Override
    protected void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {
        super.doXContentBody(builder, includeDefaults, params);
        if (includeDefaults || ignoreMalformed.explicit()) {
            builder.field(Names.IGNORE_MALFORMED, ignoreMalformed.value());
        }
    }
}
