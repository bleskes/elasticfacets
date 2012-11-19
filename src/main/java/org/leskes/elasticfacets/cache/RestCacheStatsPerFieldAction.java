/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.leskes.elasticfacets.cache;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.RestActions;
import org.elasticsearch.rest.action.support.RestXContentBuilder;

import java.io.IOException;

/**
 *
 */
public class RestCacheStatsPerFieldAction extends BaseRestHandler {

    @Inject
    public RestCacheStatsPerFieldAction(Settings settings, Client client, RestController controller) {
        super(settings, client);
        controller.registerHandler(RestRequest.Method.GET, "/_cluster/nodes/cache/fields/stats", this);
        controller.registerHandler(RestRequest.Method.GET, "/_cluster/nodes/{nodeId}/cache/fields/stats", this);

        controller.registerHandler(RestRequest.Method.GET, "/_nodes/cache/fields/stats", this);
        controller.registerHandler(RestRequest.Method.GET, "/_nodes/{nodeId}/cache/fields/stats", this);

    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel) {
        String[] nodesIds = RestActions.splitNodes(request.param("nodeId"));
        CacheStatsPerFieldRequest nodesRequest = new CacheStatsPerFieldRequest(nodesIds);
        nodesRequest.listenerThreaded(false);
        client.admin().cluster().execute(CacheStatsPerFieldAction.INSTANCE,
                nodesRequest, new ActionListener<CacheStatsPerFieldResponse>() {
            @Override
            public void onResponse(CacheStatsPerFieldResponse response) {
                try {
                    XContentBuilder builder = RestXContentBuilder.restContentBuilder(request);
                    builder.startObject();
                    response.toXContent(builder, request);
                    builder.endObject();
                    channel.sendResponse(new XContentRestResponse(request, RestStatus.OK, builder));
                } catch (Exception e) {
                    onFailure(e);
                }
            }

            @Override
            public void onFailure(Throwable e) {
                try {
                    channel.sendResponse(new XContentThrowableRestResponse(request, e));
                } catch (IOException e1) {
                    logger.error("Failed to send failure response", e1);
                }
            }
        });
    }


}