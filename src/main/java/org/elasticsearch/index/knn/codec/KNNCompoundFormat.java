package org.elasticsearch.index.knn.codec;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.CompoundFormat;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;

import java.io.IOException;
import java.util.Set;
import java.util.stream.Collectors;

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
        /**
         * If hnsw file present, remove it from compounding file.
         * We do not add header/footer to hnsw because of nmslib constraints. Compounding file expects
         * each format to present the header/footer and it fails. So we are removing from Compounding
         */

        Set<String> segmentFiles =  si.files();
        Set<String> segmentFilesExcludingHnsw =  segmentFiles.stream().filter(x -> !x.endsWith("hnsw"))
                                                             .collect(Collectors.toSet());
        si.setFiles(segmentFilesExcludingHnsw);

        /**
         * After compouding the original segment files are deleted.  As part of this our .hnsw file
         * will also be deleted. So We create new file format with ".hnswc" extension as a compound file
         * for hnsw.
         */
        dir.copyFrom(dir, si.name + ".hnsw", si.name + ".hnswc", context);
        si.setFiles(segmentFilesExcludingHnsw);
        Codec.getDefault().compoundFormat().write(dir, si, context);
    }
}
