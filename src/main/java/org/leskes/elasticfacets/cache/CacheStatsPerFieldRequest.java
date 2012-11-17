package org.leskes.elasticfacets.cache;

import org.elasticsearch.action.support.nodes.NodesOperationRequest;

public class CacheStatsPerFieldRequest extends NodesOperationRequest {

    protected CacheStatsPerFieldRequest() {
    }

    public CacheStatsPerFieldRequest(String ... nodeIds) {
        super(nodeIds);
    }
}
