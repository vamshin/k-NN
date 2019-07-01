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
//        String prefix =  String.format("%s_%s",si.name , NmsLibVersion.LATEST.buildVersion);
//        Strings hnswFile = prefix + KNNCodec.HNSW_EXTENSION;

        /**
         * If hnsw file present, remove it from compounding file.
         * We do not add header/footer to hnsw because of nmslib constraints. Compounding file expects
         * each format to present the header/footer and it fails. So we are removing from Compounding
         */
        Set<String> hnswFiles = si.files().stream().filter(file -> file.endsWith(KNNCodec.HNSW_EXTENSION))
                                     .collect(Collectors.toSet());

        Set<String> segmentFiles = new HashSet<>();
        segmentFiles.addAll(si.files());

        if(!hnswFiles.isEmpty()) {
            for(String hnswFile: hnswFiles) {
                String hnswCompoundFile = hnswFile + "c";
//                String hnswCompoundFile = prefix + KNNCodec.HNSW_COMPUND_EXTENSION;
                dir.copyFrom(dir, hnswFile, hnswCompoundFile, context);
            }

            segmentFiles.removeAll(hnswFiles);
            si.setFiles(segmentFiles);
        }

//        si.setFiles(si.files() - segmentFiles);

        /**
         *  Segment files find strings with substring hnsw. Create corresponding compound files
         */
//        if(segmentFiles.contains(hnswFile)) {
//            segmentFiles.remove(hnswFile);
//            si.setFiles(segmentFiles);
//
//            /**
//             * After compouding, the original segment files are deleted.  As part of this .hnsw file
//             * will also be deleted. So We create new file format with ".hnswc" extension as a compound file
//             * for hnsw.
//             */
//            String hnswCompoundFile = prefix + KNNCodec.HNSW_COMPUND_EXTENSION;
//            dir.copyFrom(dir, hnswFile, hnswCompoundFile, context);
//        } else {
//            logger.info("[KNN] hnsw file not present for segment %s. Ignoring hnswc", hnswFile);
//        }
        Codec.getDefault().compoundFormat().write(dir, si, context);
    }
}
