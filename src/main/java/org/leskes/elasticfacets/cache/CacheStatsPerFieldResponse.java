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

import org.elasticsearch.action.support.nodes.NodesOperationResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

/**
 *
 */
public class CacheStatsPerFieldResponse extends NodesOperationResponse<CacheStatsPerFieldStats> implements ToXContent {

    CacheStatsPerFieldResponse() {
    }

    public CacheStatsPerFieldResponse(ClusterName clusterName, CacheStatsPerFieldStats[] nodes) {
        super(clusterName, nodes);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        nodes = new CacheStatsPerFieldStats[in.readVInt()];
        for (int i = 0; i < nodes.length; i++) {
            nodes[i] = CacheStatsPerFieldStats.readCachePerFieldStats(in);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(nodes.length);
        for (CacheStatsPerFieldStats node : nodes) {
            node.writeTo(out);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("cluster_name", clusterName().value());

        builder.startObject("nodes");
        for (CacheStatsPerFieldStats nodeFieldStats : this) {
            builder.startObject(nodeFieldStats.node().id(), XContentBuilder.FieldCaseConversion.NONE);

            builder.field("setTimestamp", nodeFieldStats.timestamp());
            builder.field("name", nodeFieldStats.node().name(), XContentBuilder.FieldCaseConversion.NONE);
            builder.field("transport_address", nodeFieldStats.node().address().toString());
            if (nodeFieldStats.hostname() != null) {
                builder.field("hostname", nodeFieldStats.hostname(), XContentBuilder.FieldCaseConversion.NONE);
            }

            builder.startObject("fields");
            for (CacheStatsPerFieldStats.FieldEntry fe : nodeFieldStats.fieldSizes()){
                builder.startObject(fe.fieldName);
                builder.field("size",fe.size);
                builder.endObject();
            }
            builder.endObject(); // fields

            builder.endObject(); // node
        }
        builder.endObject();

        return builder;
    }
}