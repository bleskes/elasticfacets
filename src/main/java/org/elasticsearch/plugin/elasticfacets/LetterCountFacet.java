package org.elasticsearch.plugin.elasticfacets;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.search.facet.Facet;
import org.elasticsearch.search.facet.InternalFacet;
import org.elasticsearch.search.facet.terms.bytes.InternalByteTermsFacet;
import org.elasticsearch.search.facet.terms.bytes.InternalByteTermsFacet.ByteEntry;
import org.elasticsearch.search.facet.terms.doubles.InternalDoubleTermsFacet;

import java.io.IOException;
import java.util.List;

/**
 *
 */
public class LetterCountFacet implements InternalFacet {
	
	/**
     * The type of the filter facet.
     */
    public static final String TYPE = "lettercount";
    
    private static final String STREAM_TYPE = "lettercount";

    public static void registerStreams() {
        Streams.registerStream(STREAM, STREAM_TYPE);
    }

    static Stream STREAM = new Stream() {
        public Facet readFacet(String type, StreamInput in) throws IOException {
            return readLetterCountFacet(in);
        }
    };

    public String streamType() {
        return STREAM_TYPE;
    }

    private String name;

    private long count;

    private LetterCountFacet() {
    }

    public LetterCountFacet(String name, long count) {
        this.count = count;
        this.name = name;
    }

    public String name() {
        return this.name;
    }

    public String getName() {
        return name();
    }

    public String type() {
        return TYPE;
    }

    public String getType() {
        return TYPE;
    }

    public long count() {
        return this.count;
    }

    public long getCount() {
        return count();
    }

    static final class Fields {
        static final XContentBuilderString _TYPE = new XContentBuilderString("_type");
        static final XContentBuilderString COUNT = new XContentBuilderString("count");
    }

    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(name);
        builder.field(Fields._TYPE, LetterCountFacet.TYPE);
        builder.field(Fields.COUNT, count());
        builder.endObject();
        return builder;
    }

    public static LetterCountFacet readLetterCountFacet(StreamInput in) throws IOException {
        LetterCountFacet facet = new LetterCountFacet();
        facet.readFrom(in);
        return facet;
    }

    public void readFrom(StreamInput in) throws IOException {
        name = in.readString();
        count = in.readVLong();
    }

    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeVLong(count);
    }

	public Facet reduce(String name, List<Facet> facets) {
		if (facets.size() == 1) {
            return facets.get(0);
        }
		LetterCountFacet first = (LetterCountFacet) facets.get(0);
		long count = 0;
        for (Facet facet : facets) {
        	count += ((LetterCountFacet)facet).count;
        }

        first.count = count;
		return first;
	}
}
