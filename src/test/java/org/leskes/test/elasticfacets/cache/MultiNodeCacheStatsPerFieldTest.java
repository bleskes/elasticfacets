package org.leskes.test.elasticfacets.cache;

public class MultiNodeCacheStatsPerFieldTest extends CacheStatsPerFieldTest {

    protected int numberOfNodes() {
        return 3;
    }

    protected int numberOfShards() {
        return 3;
    }


}
