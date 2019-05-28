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

package org.elasticsearch.index.knn.codec;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IndexOutput;
import org.elasticsearch.index.knn.KNNIndex;
import org.elasticsearch.index.knn.KNNVectorFieldMapper;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Paths;
import java.security.AccessController;
import java.security.PrivilegedAction;

/**
 * This class writes the KNN docvalues to the segments
 */
class KNNDocValuesConsumer extends DocValuesConsumer implements Closeable {

    private final Logger logger = LogManager.getLogger(KNNDocValuesConsumer.class);

    private DocValuesConsumer delegatee;
    private SegmentWriteState state;

    KNNDocValuesConsumer(DocValuesConsumer delegatee, SegmentWriteState state) {
        this.delegatee = delegatee;
        this.state = state;
    }

    @Override
    public void addBinaryField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
        delegatee.addBinaryField(field, valuesProducer);

        //TODO Non blocking I/O https://issues.amazon.com/issues/CloudSearch-8557
        if (field.attributes().containsKey(KNNVectorFieldMapper.KNN_FIELD)) {
            BinaryDocValues values = valuesProducer.getBinary(field);
            String indexPath = Paths.get(((FSDirectory) (FilterDirectory.unwrap(state.directory))).getDirectory().toString(),
                    String.format("%s.hnsw", state.segmentInfo.name)).toString();

            /**
             * Touch the file so that this gets lifecycle for free
             */
            IndexOutput data;
            data = state.directory.createOutput(IndexFileNames.segmentFileName(state.segmentInfo.name, "", "hnsw"), state.context);
            data.close();

            KNNCodec.Pair pair = KNNCodec.getFloats(values);
            AccessController.doPrivileged(
                    new PrivilegedAction<Void>() {
                        public Void run() {
                            KNNIndex.saveIndex(pair.docs, pair.vectors, indexPath);
                            return null;
                        }
                    }
            );
        }
    }

    /**
     * Merges in the fields from the readers in mergeState
     *
     * @param mergeState Holds common state used during segment merging
     */
    @Override
    public void merge(MergeState mergeState) {
        try {
            delegatee.merge(mergeState);
            assert mergeState != null;
            assert mergeState.mergeFieldInfos != null;
            for (FieldInfo fieldInfo : mergeState.mergeFieldInfos) {
                DocValuesType type = fieldInfo.getDocValuesType();
                if (type == DocValuesType.BINARY) {
                    addBinaryField(fieldInfo, new KNNDocValuesProducer(mergeState));
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void addSortedSetField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
        delegatee.addSortedSetField(field, valuesProducer);
    }

    @Override
    public void addSortedNumericField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
        delegatee.addSortedNumericField(field, valuesProducer);
    }

    @Override
    public void addSortedField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
        delegatee.addSortedField(field, valuesProducer);
    }

    @Override
    public void addNumericField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
        delegatee.addNumericField(field, valuesProducer);
    }

    @Override
    public void close() throws IOException {

        delegatee.close();
    }
}
