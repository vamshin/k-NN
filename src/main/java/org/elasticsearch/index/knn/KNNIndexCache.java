package org.elasticsearch.index.knn;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.cache.RemovalListener;
import org.elasticsearch.common.cache.RemovalNotification;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.knn.v1736.KNNIndex;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ExecutionException;
import java.util.function.ToLongBiFunction;

public class KNNIndexCache implements RemovalListener<String, KNNIndex>, Releasable {

    private static Logger logger = LogManager.getLogger(KNNIndexCache.class);
    // TODO Expose these as Elasticsearch settings
    final long sizeInBytes = 0;
    boolean timestampEnabled = true;
    public static Cache<String, KNNIndex> cache;

    private static KNNIndexFileListener knnIndexFileListener = null;

    public static void setKnnIndexFileListener(KNNIndexFileListener knnIndexFileListener) {
        KNNIndexCache.knnIndexFileListener = knnIndexFileListener;
    }

    public KNNIndexCache() {
        CacheBuilder<String, KNNIndex> cacheBuilder = CacheBuilder.<String, KNNIndex>builder()
                                                                 .removalListener(this);
        if (sizeInBytes > 0) {
            cacheBuilder.setMaximumWeight(sizeInBytes).weigher(new KNNIndexWeight());
        }

        if(timestampEnabled) {
            cacheBuilder.setExpireAfterAccess(TimeValue.timeValueMinutes(20)).setExpireAfterWrite(TimeValue.timeValueSeconds(60));
        }
        cache = cacheBuilder.build();
    }

    @Override
    public void close() {

    }

    @Override
    public void onRemoval(RemovalNotification<String, KNNIndex> removalNotification) {
        try {
            //TODO make it debug
            logger.info("[KNN] Cache evicted. Key " + removalNotification.getKey()
                                 + " Reason: " + removalNotification.getRemovalReason());
            KNNIndex knnIndex = removalNotification.getValue();
            knnIndex.gc();
            // This flag is to ensure, callers already holding the object do not query if the flag i
            // is set
            knnIndex.isDeleted = true;
        } catch(Exception ex) {
            logger.error("Exception occured while performing gc for hnsw index " + removalNotification.getKey());
        }
    }


    public void addEntry(String key, KNNIndex value) {
        cache.put(key, value);
    }


    public KNNIndex getIndex(String key) {
        try {
            KNNIndex knnIndex =cache.computeIfAbsent(key, indexPathUrl -> computeIndex(indexPathUrl));
            return knnIndex;
        } catch (ExecutionException e) {
            logger.error("Exception occured while computing the index. Skipped Adding to cache");
        }
        return null;
    }


    public KNNIndex computeIndex(String indexPathUrl) throws Exception {
        Path indexPath = Paths.get(indexPathUrl);
        knnIndexFileListener.register(indexPath);
        return KNNIndex.loadIndex(indexPathUrl);
    }

    class KNNIndexWeight implements ToLongBiFunction<String, KNNIndex> {
        @Override
        public long applyAsLong(String s, KNNIndex knnIndex) {
            return knnIndex.getIndexSize();
        }
    }
}

