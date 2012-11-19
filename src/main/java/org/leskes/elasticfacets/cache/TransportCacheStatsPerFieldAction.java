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

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.support.nodes.NodeOperationRequest;
import org.elasticsearch.action.support.nodes.TransportNodesOperationAction;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.network.NetworkUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.cache.field.data.FieldDataCache;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.net.InetAddress;
import java.util.List;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 *
 */
public class TransportCacheStatsPerFieldAction extends TransportNodesOperationAction<CacheStatsPerFieldRequest,
        CacheStatsPerFieldResponse, TransportCacheStatsPerFieldAction.CacheStatsPerFieldStatsRequest,
        CacheStatsPerFieldStats> {


    @Nullable
    private String hostname;

    private IndicesService indicesService;

    @Inject
    public TransportCacheStatsPerFieldAction(Settings settings, ClusterName clusterName, ThreadPool threadPool,
                                             ClusterService clusterService, TransportService transportService,
                                             IndicesService indicesService
                                            ) {
        super(settings, clusterName, threadPool, clusterService, transportService);
        this.indicesService = indicesService;
        InetAddress address = NetworkUtils.getLocalAddress();
        if (address != null) {
            this.hostname = address.getHostName();
        }

    }

    @Override
    protected String executor() {
        return ThreadPool.Names.MANAGEMENT;
    }

    @Override
    protected CacheStatsPerFieldRequest newRequest() {
        return new CacheStatsPerFieldRequest();
    }

    @Override
    protected String transportAction() {
        return CacheStatsPerFieldAction.NAME;
    }

    @Override
    protected CacheStatsPerFieldResponse newResponse(CacheStatsPerFieldRequest request, AtomicReferenceArray responses) {
        final List<CacheStatsPerFieldStats> nodeStats = Lists.newArrayList();
        for (int i = 0; i < responses.length(); i++) {
            Object resp = responses.get(i);
            if (resp instanceof CacheStatsPerFieldStats) {
                nodeStats.add((CacheStatsPerFieldStats) resp);
            }
        }
        return new CacheStatsPerFieldResponse(clusterName, nodeStats.toArray(new CacheStatsPerFieldStats[nodeStats.size()]));
    }


    @Override
    protected CacheStatsPerFieldStatsRequest newNodeRequest() {
        return new CacheStatsPerFieldStatsRequest();
    }

    @Override
    protected CacheStatsPerFieldStatsRequest newNodeRequest(String nodeId, CacheStatsPerFieldRequest request) {
        return new CacheStatsPerFieldStatsRequest(nodeId, request);
    }

    @Override
    protected CacheStatsPerFieldStats newNodeResponse() {
        return new CacheStatsPerFieldStats();
    }


    @Override
    protected CacheStatsPerFieldStats nodeOperation(CacheStatsPerFieldStatsRequest nodeStatsRequest) throws ElasticSearchException {
        List<CacheStatsPerFieldStats.FieldEntry> entries = Lists.newArrayList();
        for (IndexService indexService : indicesService) {
            logger.debug("Starting to analyze index {}",indexService.settingsService().index().name());
            FieldDataCache fieldData = indexService.cache().fieldData();
            for (DocumentMapper mapper : indexService.mapperService()) {
                for (FieldMapper fieldMapper : mapper.mappers()) {
                    String field = fieldMapper.names().indexName();
                    logger.debug("Calculating size for field {}",field);
                    long size = fieldData.sizeInBytes(field);
                    if (size > 0) {
                        logger.debug("Size for field {}: {}",field,size);
                        entries.add(new CacheStatsPerFieldStats.FieldEntry(field,size));
                    }
                    else logger.debug("Field {} is has no cache. Skipping.",field);
                }
            }
        }
        return new CacheStatsPerFieldStats(clusterService.localNode(),hostname, System.currentTimeMillis(),entries);
    }

    @Override
    protected boolean accumulateExceptions() {
        return false;
    }

    static class CacheStatsPerFieldStatsRequest extends NodeOperationRequest {

        CacheStatsPerFieldRequest request;

        CacheStatsPerFieldStatsRequest() {
        }

        CacheStatsPerFieldStatsRequest(String nodeId, CacheStatsPerFieldRequest request) {
            super(nodeId);
            this.request = request;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            request = new CacheStatsPerFieldRequest();
            request.readFrom(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            request.writeTo(out);
        }
    }
}