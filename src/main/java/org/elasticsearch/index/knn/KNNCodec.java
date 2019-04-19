package org.elasticsearch.index.knn;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.codecs.*;
import org.apache.lucene.codecs.lucene70.Lucene70Codec;
import org.apache.lucene.codecs.perfield.PerFieldDocValuesFormat;
import org.apache.lucene.index.*;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.SpecialPermission;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.file.Paths;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.List;

class BinaryDocValuesSub extends DocIDMerger.Sub {

    private final BinaryDocValues values;

    public BinaryDocValues getValues() {
        return values;
    }

    public BinaryDocValuesSub(MergeState.DocMap docMap, BinaryDocValues values) {
        super(docMap);
        assert values != null;
        this.values = values;
        //assert values.docID() == -1;
    }

    @Override
    public int nextDoc() throws IOException {
        return values.nextDoc();
    }
}

class KNNBinaryDocValues extends BinaryDocValues {

    private DocIDMerger<BinaryDocValuesSub> docIDMerger;

    public KNNBinaryDocValues(DocIDMerger<BinaryDocValuesSub> docIdMerger) {
        this.docIDMerger = docIdMerger;
    }

    private BinaryDocValuesSub current;
    private int docID = -1;

    @Override
    public int docID() {
        return docID;
    }

    @Override
    public int nextDoc() throws IOException {
        current = docIDMerger.next();
        if (current == null) {
            docID = NO_MORE_DOCS;
        } else {
            docID = current.mappedDocID;
        }
        return docID;
    }

    @Override
    public int advance(int target) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean advanceExact(int target) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public long cost() {
        throw new UnsupportedOperationException();
    }

    @Override
    public BytesRef binaryValue() throws IOException {
        return current.getValues().binaryValue();
    }
};

class KNNDocValuesProducer extends EmptyDocValuesProducer {

    private MergeState mergeState;

    public KNNDocValuesProducer(MergeState mergeState) {
        this.mergeState = mergeState;
    }

    @Override
    public BinaryDocValues getBinary(FieldInfo field) {
        try {
            List<BinaryDocValuesSub> subs = new ArrayList<>(this.mergeState.docValuesProducers.length);
            for (int i = 0; i < this.mergeState.docValuesProducers.length; i++) {
                subs.add(new BinaryDocValuesSub(mergeState.docMaps[i], this.mergeState.docValuesProducers[i].getBinary(field)));
            }
            return new KNNBinaryDocValues(DocIDMerger.of(subs, mergeState.needsIndexSort));
        } catch(Exception e) {
            throw new RuntimeException(e);
        }
    }
}

class KNNDocValuesConsumer extends DocValuesConsumer implements Closeable {

    private final MapperService mapperService;
    private final Logger logger;


    public void addBinaryField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
        // No need to write this to base after KNN is fixed to not expect values while loading. It seems to be
        // used for merging as well which should be followed up with AI team.
        delegatee.addBinaryField(field, valuesProducer);

     // TODO: Need to this only for KNN field, but SPI can't use non-arg ctr :(
     //   final MappedFieldType fieldType = mapperService.fullName(field.name);
     //   if (fieldType != null && fieldType instanceof KNNVectorFieldMapper.KNNVectorFieldType) {
            BinaryDocValues values = valuesProducer.getBinary(field);
            String indexPath = Paths.get(((FSDirectory) (FilterDirectory.unwrap(state.directory))).getDirectory().toString(), String.format("%s.hnsw", state.segmentInfo.name)).toString();
            KNNCodec.Pair pair = KNNCodec.getFloats(values);
            SecurityManager sm = System.getSecurityManager();
            if (sm != null) {
                // unprivileged code such as scripts do not have SpecialPermission
                sm.checkPermission(new SpecialPermission());
            }
            AccessController.doPrivileged(
                new PrivilegedAction<Void>() {
                    public Void run() {
                        KNNIndex.saveIndex(pair.docs, pair.vectors, indexPath);
                        return null;
                    }
                }
            );
     //   }
    }

    @Override
    public void merge(MergeState mergeState) {
        try {
            delegatee.merge(mergeState);assert mergeState != null;
            assert mergeState.mergeFieldInfos != null;
            for (FieldInfo fieldInfo : mergeState.mergeFieldInfos) {
                DocValuesType type = fieldInfo.getDocValuesType();
                if (type == DocValuesType.BINARY) {
                    addBinaryField(fieldInfo, new KNNDocValuesProducer(mergeState));
                }
            }
        } catch(Exception e) {
            throw new RuntimeException(e);
        }
    }

    private DocValuesConsumer delegatee;
    private SegmentWriteState state;

    public KNNDocValuesConsumer(DocValuesConsumer delegatee, SegmentWriteState state, MapperService mapperService, Logger logger) {
        this.delegatee = delegatee;
        this.state = state;
        this.mapperService = mapperService;
        this.logger = logger;
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

class KNNDocValuesFormat extends DocValuesFormat {
    private final Logger logger;
    private final MapperService mapperService;

    public KNNDocValuesFormat(MapperService mapperService, Logger logger) {
        super("Lucene70");
        this.mapperService = mapperService;
        this.logger = logger;
    }

    private DocValuesFormat delegate = DocValuesFormat.forName("Lucene70");

    @Override
    public DocValuesConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
        return new KNNDocValuesConsumer(delegate.fieldsConsumer(state), state, mapperService, logger);
    }

    @Override
    public DocValuesProducer fieldsProducer(SegmentReadState state) throws IOException {
        return delegate.fieldsProducer(state);
    }
}

public class KNNCodec extends Codec {
    private final DocValuesFormat docValuesFormat;
    private final DocValuesFormat perFieldDocValuesFormat;
    private final Logger logger;

    private Lucene70Codec delegatee = new Lucene70Codec();

    public KNNCodec() {
        super("KNNCodec");
        this.logger = null;
        MapperService mapperService = null;
        docValuesFormat = new KNNDocValuesFormat(mapperService, logger);
        perFieldDocValuesFormat = new PerFieldDocValuesFormat() {
            @Override
            public DocValuesFormat getDocValuesFormatForField(String field) {
                return docValuesFormat;
            }
        };
    }

    @Override
    public PostingsFormat postingsFormat() {
        return delegatee.postingsFormat();
    }

    @Override
    public DocValuesFormat docValuesFormat() {
        return this.perFieldDocValuesFormat;
    }

    @Override
    public StoredFieldsFormat storedFieldsFormat() {
        return delegatee.storedFieldsFormat();
    }

    @Override
    public TermVectorsFormat termVectorsFormat() {
        return delegatee.termVectorsFormat();
    }

    @Override
    public FieldInfosFormat fieldInfosFormat() {
        return delegatee.fieldInfosFormat();
    }

    @Override
    public SegmentInfoFormat segmentInfoFormat() {
        return delegatee.segmentInfoFormat();
    }

    @Override
    public NormsFormat normsFormat() {
        return delegatee.normsFormat();
    }

    @Override
    public LiveDocsFormat liveDocsFormat() {
        return delegatee.liveDocsFormat();
    }

    @Override
    public CompoundFormat compoundFormat() {
        return delegatee.compoundFormat();
    }

    @Override
    public PointsFormat pointsFormat() {
        return delegatee.pointsFormat();
    }

    public static final class Pair {
        public Pair(int[] docs, float[][] vectors) {
            this.docs = docs;
            this.vectors = vectors;
        }
        public int[] docs;
        public float[][] vectors;
    };

    public static Pair getFloats(BinaryDocValues values) throws IOException {
        ArrayList<float[]> vectorList = new ArrayList<>();
        ArrayList<Integer> docIdList = new ArrayList<>();
        for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
            byte[] value = values.binaryValue().bytes;

            try (ByteArrayInputStream byteStream = new ByteArrayInputStream(value);
                 ObjectInputStream objectStream = new ObjectInputStream(byteStream)) {
                float[] vector = (float[]) objectStream.readObject();
                vectorList.add(vector);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
            docIdList.add(doc);
        }
        return new Pair(docIdList.stream().mapToInt(Integer::intValue).toArray(), vectorList.toArray(new float[][]{}));
    }
}
