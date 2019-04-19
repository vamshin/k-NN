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

import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryShardContext;


import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * KNN query is a query that search for the approximate K-nearest neighbors
 */
public class KNNQueryBuilder extends AbstractQueryBuilder<KNNQueryBuilder> {
    public static final ParseField VECTOR_FIELD = new ParseField("vector");
    public static final ParseField K_FIELD = new ParseField("k");
    /**
     * The name for the knn query
     */
    public static final String NAME = "knn";
    /**
     * The default mode terms are combined in a match query
     */
    public static Logger logger = Loggers.getLogger(KNNQueryBuilder.class);
    private final String fieldName;
    private final float[] vector;
    private int k = 0;

    /**
     * Constructs a new match query.
     */
    public KNNQueryBuilder(String fieldName, float[] vector, int k) {
        if (fieldName == null) {
            throw new IllegalArgumentException("[" + NAME + "] requires fieldName");
        }
        if (vector == null) {
            throw new IllegalArgumentException("[" + NAME + "] requires query vector");
        }
        if (k == 0) {
            throw new IllegalArgumentException("[" + NAME + "] requires k > 0");
        }
        this.fieldName = fieldName;
        this.vector = vector;
        this.k = k;
    }

    /**
     * Read from a stream.
     */
    public KNNQueryBuilder(StreamInput in) throws IOException {
        super(in);
        fieldName = in.readString();
        vector = in.readFloatArray();
        k = in.readInt();
    }

    static private float[] ObjectsToFloats(List<Object> objs) {
        float[] vec = new float[objs.size()];
        for (int i = 0; i < objs.size(); i++) {
            vec[i] = ((Number)objs.get(i)).floatValue();
        }
        return vec;
    }

    public static KNNQueryBuilder fromXContent(XContentParser parser) throws IOException {
        //logger.info("fromXContent");
        String fieldName = null;
        List<Object> vector = null;
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;
        int k = 0;
        String queryName = null;
        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                throwParsingExceptionOnMultipleFields(NAME, parser.getTokenLocation(), fieldName, currentFieldName);
                fieldName = currentFieldName;
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        currentFieldName = parser.currentName();
                    } else if (token.isValue() || token == XContentParser.Token.START_ARRAY) {
                        if (VECTOR_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            vector = parser.list();
                        } else if (AbstractQueryBuilder.BOOST_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            boost = parser.floatValue();
                        } else if (K_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            k = parser.intValue();
                        } else if (AbstractQueryBuilder.NAME_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            queryName = parser.text();
                        } else {
                            throw new ParsingException(parser.getTokenLocation(),
                                "[" + NAME + "] query does not support [" + currentFieldName + "]");
                        }
                    } else {
                        throw new ParsingException(parser.getTokenLocation(),
                            "[" + NAME + "] unknown token [" + token + "] after [" + currentFieldName + "]");
                    }
                }
            } else {
                throwParsingExceptionOnMultipleFields(NAME, parser.getTokenLocation(), fieldName, parser.currentName());
                fieldName = parser.currentName();
                vector = parser.list();
            }
        }

        if (vector == null) {
            throw new ParsingException(parser.getTokenLocation(), "No vector specified for knn query");
        }
        if (k == 0) {
            throw new ParsingException(parser.getTokenLocation(), "k in knn query should be positive integer");
        }
        //logger.info("vector = " + vector.toString());
        //logger.info("k = " + String.valueOf(k));
        //System.out.println("vector = " + vector.toString());
        //System.out.println("k = " + String.valueOf(k));

        KNNQueryBuilder knnQuery = new KNNQueryBuilder(fieldName, ObjectsToFloats(vector), k);
        knnQuery.queryName(queryName);
        knnQuery.boost(boost);
        return knnQuery;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(fieldName);
        out.writeFloatArray(vector);
        out.writeInt(k);
    }

    /**
     * Returns the field name used in this query.
     */
    public String fieldName() {
        return this.fieldName;
    }

    /**
     * Returns the value used in this query.
     */
    public Object vector() {
        return this.vector;
    }

    @Override
    public void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.startObject(fieldName);

        builder.field(VECTOR_FIELD.getPreferredName(), vector);
        builder.field(K_FIELD.getPreferredName(), k);
        printBoostAndQueryName(builder);
        builder.endObject();
        builder.endObject();
    }


    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        Query query = null;
        //query = new KNNQuery(this.fieldName, vector, k);
        query = new KNNQuery(this.fieldName, vector, k, logger);
        return query;
    }

    @Override
    protected boolean doEquals(KNNQueryBuilder other) {
        return Objects.equals(fieldName, other.fieldName) &&
            Objects.equals(vector, other.vector) &&
            Objects.equals(k, other.k);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(fieldName, vector, k);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }
}
