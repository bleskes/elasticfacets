package org.leskes.elasticfacets.cache;

import org.elasticsearch.action.support.nodes.NodesOperationRequest;

public class CacheStatsPerFieldRequest extends NodesOperationRequest<CacheStatsPerFieldRequest> {

    protected CacheStatsPerFieldRequest() {
    }

    public CacheStatsPerFieldRequest(String ... nodeIds) {
        super(nodeIds);
    }
}
