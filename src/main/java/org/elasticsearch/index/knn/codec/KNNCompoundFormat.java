package org.elasticsearch.index.knn.codec;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.CompoundFormat;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.elasticsearch.index.knn.util.NmsLibVersion;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Class to encode/decode compound file
 */
public class KNNCompoundFormat extends CompoundFormat {


    KNNCompoundFormat() {
    }

    @Override
    public Directory getCompoundReader(Directory dir, SegmentInfo si, IOContext context) throws IOException {
        return Codec.getDefault().compoundFormat().getCompoundReader(dir, si, context);
    }

    @Override
    public void write(Directory dir, SegmentInfo si, IOContext context) throws IOException {
        String prefix =  String.format("%s_%s",si.name , NmsLibVersion.LATEST.value);
        String hnswFile = prefix + ".hnsw";

        /**
         * If hnsw file present, remove it from compounding file.
         * We do not add header/footer to hnsw because of nmslib constraints. Compounding file expects
         * each format to present the header/footer and it fails. So we are removing from Compounding
         */

        Set<String> segmentFiles =  new HashSet<>();
        segmentFiles.addAll(si.files());
        if(segmentFiles.contains(hnswFile)) {
            segmentFiles.remove(hnswFile);
            si.setFiles(segmentFiles);

            /**
             * After compouding the original segment files are deleted.  As part of this our .hnsw file
             * will also be deleted. So We create new file format with ".hnswc" extension as a compound file
             * for hnsw.
             */
            dir.copyFrom(dir, hnswFile, prefix + ".hnswc", context);
        }

        Codec.getDefault().compoundFormat().write(dir, si, context);
    }
}
