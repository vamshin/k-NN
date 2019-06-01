package org.elasticsearch.index.knn.util;

public enum NmsLibVersion {

    /**
     * Latest available nmslib version
     * When new nmslib is build, please make sure to update LATEST
     */
    LATEST("1736"){
        @Override
        public String version() {
            return "KNNIndexV1_7_3_6";
        }
    },
    V1736("1736"){
        @Override
        public String version() {
            return "KNNIndexV1_7_3_6";
        }
    };

    public String value;

    NmsLibVersion(String value) {
        this.value = value;
    }

    /**
     * NMS library version used by the KNN codec
     * @return nmslib name
     */
    public abstract String version();
}
