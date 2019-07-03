package org.elasticsearch.index.knn.codec;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.CompoundFormat;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Class to encode/decode compound file
 */
public class KNNCompoundFormat extends CompoundFormat {

    private final Logger logger = LogManager.getLogger(KNNCompoundFormat.class);

    KNNCompoundFormat() {
    }

    @Override
    public Directory getCompoundReader(Directory dir, SegmentInfo si, IOContext context) throws IOException {
        return Codec.getDefault().compoundFormat().getCompoundReader(dir, si, context);
    }

    @Override
    public void write(Directory dir, SegmentInfo si, IOContext context) throws IOException {
        /**
         * If hnsw file present, remove it from compounding file.
         * We do not add header to hnsw because of nmslib constraints.
         */
        Set<String> hnswFiles = si.files().stream().filter(file -> file.endsWith(KNNCodec.HNSW_EXTENSION))
                                     .collect(Collectors.toSet());

        Set<String> segmentFiles = new HashSet<>();
        segmentFiles.addAll(si.files());

        if(!hnswFiles.isEmpty()) {
            for(String hnswFile: hnswFiles) {
                String hnswCompoundFile = hnswFile + "c";
                dir.copyFrom(dir, hnswFile, hnswCompoundFile, context);
            }
            segmentFiles.removeAll(hnswFiles);
            si.setFiles(segmentFiles);
        }
        Codec.getDefault().compoundFormat().write(dir, si, context);
    }
}
