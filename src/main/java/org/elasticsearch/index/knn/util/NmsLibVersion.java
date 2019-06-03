package org.elasticsearch.index.knn.util;

public enum NmsLibVersion {

    /**
     * Latest available nmslib version
     */
    LATEST("1736"){
        @Override
        public String indexLibraryVersion() {
            return "KNNIndexV1_7_3_6";
        }
    },
    V1736("1736"){
        @Override
        public String indexLibraryVersion() {
            return "KNNIndexV1_7_3_6";
        }
    };

    public String buildVersion;

    NmsLibVersion(String buildVersion) {
        this.buildVersion = buildVersion;
    }

    /**
     * NMS library version used by the KNN codec
     * @return nmslib name
     */
    public abstract String indexLibraryVersion();
}
