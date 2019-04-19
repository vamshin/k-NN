Elasticsearch-KNN
=================

Query K nearest neighbours for the given vector.

### Building in Brazil

To build in brazil we need to prefetch our dependencies and upload it to Brazil's S3 using [S3BinarySafeDownloader](https://code.amazon.com/packages/S3BinarySafeDownloader/trees/mainline). This needs to be done every time a new dependency is added or a dependency version is upgraded.  During the build the dependencies are automatically fetched and unpacked into a local maven repo and the build is configured to use that repo instead of downloading dependencies from mavenCentral. To update a dependency, add it to the relevant build.gradle and then run the following commands:

```
./gradlew -P offlineRepo=true prepareOfflineRepo
brazil-build-tool-exec s3-safe-upload buildSrc/es-knn-offline-repo-${VERSION}.zip
brazil-build-tool-exec s3-safe-upload buildSrc/libs/offline-dependencies-plugin-thirdparty-repackaged-all-unspecified.jar
git commit -a -m 'Add dependency <XXX> to offline repo'
```

### Change elasticsearch version
Update `es.mv` and `es.version` in [gradle.properties](gradle.properties) and [brazil.gradle](build-tools/brazil.gradle)

## SETUP
<TODO> 
