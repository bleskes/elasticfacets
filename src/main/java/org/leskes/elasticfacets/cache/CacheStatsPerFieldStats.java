package org.leskes.elasticfacets.cache;

import org.elasticsearch.action.support.nodes.NodeOperationResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CacheStatsPerFieldStats extends NodeOperationResponse {

    public static class FieldEntry {
        public final String fieldName;
        public final long size;

        public FieldEntry(String fieldName, long size) {
            this.fieldName = fieldName;
            this.size = size;
        }
    }

    private long timestamp;
    private List<FieldEntry> fieldSizes;
    private String hostname;

    CacheStatsPerFieldStats() {
    }

    public CacheStatsPerFieldStats(DiscoveryNode node, String hostname, long timestamp, List<FieldEntry> fieldSizes) {
        super(node);
        this.setHostname(hostname);
        this.timestamp = timestamp;
        this.fieldSizes = fieldSizes;
    }

    public static CacheStatsPerFieldStats readCachePerFieldStats(StreamInput in) throws IOException {
        CacheStatsPerFieldStats nodeInfo = new CacheStatsPerFieldStats();
        nodeInfo.readFrom(in);
        return nodeInfo;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        setTimestamp(in.readVLong());
        if (in.readBoolean()) {
            hostname = in.readString();
        }
        int size = in.readVInt();
        fieldSizes = new ArrayList<FieldEntry>(size);
        for(int i=0;i<size;i++) {
            FieldEntry e = new FieldEntry(in.readString(),in.readVLong());
            fieldSizes.add(e);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVLong(timestamp());
        if (hostname == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeString(hostname);
        }
        out.writeVInt(fieldSizes.size());
        for (FieldEntry e: fieldSizes) {
            out.writeString(e.fieldName);
            out.writeVLong(e.size);
        }
    }

    public long timestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public List<FieldEntry> fieldSizes() {
        return fieldSizes;
    }

    public void setFieldSizes(List<FieldEntry> fieldSizes) {
        this.fieldSizes = fieldSizes;
    }

    public String hostname() {
        return hostname;
    }

    public void setHostname(String hostname) {
        this.hostname = hostname;
    }


}
