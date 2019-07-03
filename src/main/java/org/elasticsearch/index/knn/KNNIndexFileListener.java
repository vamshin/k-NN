package org.elasticsearch.index.knn;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.watcher.FileChangesListener;
import org.elasticsearch.watcher.FileWatcher;
import org.elasticsearch.watcher.ResourceWatcherService;

import java.io.IOException;
import java.nio.file.Path;

/**
 * File Listener class to perform hsnw index garbage collection when the corresponding
 * segments get deleted
 */
public class KNNIndexFileListener implements FileChangesListener {
    private static Logger logger = LogManager.getLogger(KNNIndexFileListener.class);

    private ResourceWatcherService resourceWatcherService;

    public KNNIndexFileListener(ResourceWatcherService resourceWatcherService) {
        this.resourceWatcherService= resourceWatcherService;
    }

    public void register(Path filePath) throws Exception {

        final FileWatcher watcher = new FileWatcher(filePath);
        watcher.addListener(this);
        watcher.init();
        try {
            resourceWatcherService.add(watcher, ResourceWatcherService.Frequency.HIGH);

        } catch (IOException e) {
            logger.error("couldn't initialize resource watcher for file " + filePath.toString(), e);
        }
        logger.debug("[KNN] Registered file {}", filePath.toString());
    }

    @Override
    public void onFileDeleted(Path indexFilePath) {
        logger.debug("[KNN] Invalidated because file {} is deleted", indexFilePath.toString());
        KNNWeight.knnIndexCache.cache.invalidate(indexFilePath.toString());
    }
}
