package org.leskes.elasticfacets;

import org.elasticsearch.common.CacheRecycler;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.mvel2.optimizers.impl.refl.nodes.ArrayLength;
import org.elasticsearch.common.trove.ExtTLongObjectHashMap;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.search.facet.Facet;
import org.elasticsearch.search.facet.FacetCollector;
import org.elasticsearch.search.facet.FacetProcessors;
import org.elasticsearch.search.facet.datehistogram.InternalDateHistogramFacet;
import org.elasticsearch.search.facet.datehistogram.DateHistogramFacet.Entry;
import org.elasticsearch.search.facet.datehistogram.InternalFullDateHistogramFacet.FullEntry;
import org.elasticsearch.search.facet.InternalFacet;

import java.io.IOException;
import java.util.*;

/**
 *
 */
public class FacetedDateHistogramFacet implements InternalFacet {

    private static final String STREAM_TYPE = "facetedDateHistogram";

    public static void registerStreams() {
        Streams.registerStream(STREAM, STREAM_TYPE);
    }

    static Stream STREAM = new Stream() {
        public Facet readFacet(String type, StreamInput in) throws IOException {
            return readFacetedHistogramFacet(in);
        }
    };

    public String streamType() {
        return STREAM_TYPE;
    }
    
    public static final String TYPE = "faceted_date_histogram";
    
    
    protected static final Comparator<EntryBase> comparator = new Comparator<EntryBase>() {

        public int compare(EntryBase o1, EntryBase o2) {
            // push nulls to the end
            if (o1 == null) {
                if (o2 == null) {
                    return 0;
                }
                return 1;
            }
            if (o2 == null) {
                return -1;
            }
            return (o1.time < o2.time ? -1 : (o1.time == o2.time ? 0 : 1));
        }
    };


    public static class EntryBase {
    	public final long time;
    	
    	public EntryBase(long time) {
    		this.time = time;
    	}
    	
    };

    /**
     * A histogram entry representing a single entry within the result of a histogram facet.
     */
    public static class Entry extends EntryBase {
        public InternalFacet internalFacet; 
        public FacetCollector collector;

        public Entry(long time, FacetCollector collector) {
        	super(time);
            this.collector = collector;
        }
        public Entry(long time) {
        	this(time,null);

        }
        
        public void facetize() {
        	this.internalFacet = (InternalFacet)collector.facet();
        	this.collector = null;
        }

    }
    
    /**
     * Entry which can contain multiple facts per entry. used for reducing
     */
    public static class MultiEntry extends EntryBase {
    	public List<Facet> facets;
    	
    	public MultiEntry(long time) {
        	super(time);
    		this.facets = new ArrayList<Facet>();
    	}
    }


    private String name;
    

    protected ExtTLongObjectHashMap<Entry> entries;
    
    protected List<Entry> entriesAsList;
    

    public List<Entry> collapseToAList() {
        if (!(entriesAsList instanceof List)) {
        	entriesAsList = new ArrayList<Entry>(entries.valueCollection());
            releaseEntries();
        }
        return entriesAsList;
    }
    

    private FacetedDateHistogramFacet() {
    }

    public FacetedDateHistogramFacet(String name, ExtTLongObjectHashMap<Entry> entries) {
    	// Now we own the entries map. It is MUST come from the cache recycler..
        this.name = name;
        
        this.entries = entries;
    }

    void releaseEntries() {
    	if (entries != null) {
	        CacheRecycler.pushLongObjectMap(entries);
	        entries = null;
    	}
    }



    public Facet reduce(String name, List<Facet> facets,FacetProcessors facetProcessors) {
        if (facets.size() == 1) {
            // we need to sort it
            FacetedDateHistogramFacet internalFacet = (FacetedDateHistogramFacet) facets.get(0);
            List<Entry> entries = internalFacet.collapseToAList();
            Collections.sort(entries, comparator);
            return internalFacet;
        }

        ExtTLongObjectHashMap<MultiEntry> map = CacheRecycler.popLongObjectMap();

        for (Facet facet : facets) {
        	FacetedDateHistogramFacet histoFacet = (FacetedDateHistogramFacet) facet;
            for (Entry entry : histoFacet.collapseToAList()) {
            	MultiEntry current = map.get(entry.time);
                if (current == null) {
                	current = new MultiEntry(entry.time);
                    map.put(current.time, current);
                }
                current.facets.add(entry.internalFacet);
            }
        }

        // sort
        Object[] values = map.internalValues();
        Arrays.sort(values, (Comparator) comparator);
        List<MultiEntry> ordered = new ArrayList<MultiEntry>(map.size());
        for (int i = 0; i < map.size(); i++) {
        	MultiEntry value = (MultiEntry) values[i];
            if (value == null) {
                break;
            }
            ordered.add(value);
        }

        CacheRecycler.pushLongObjectMap(map);

        // just initialize it as already ordered facet
        FacetedDateHistogramFacet ret = new FacetedDateHistogramFacet();
        ret.name = name;
        ret.entriesAsList = new ArrayList<Entry>(ordered.size());
        
        for (MultiEntry me : ordered) {
        	Entry e = new Entry(me.time);
        	Facet f = me.facets.get(0);
        	e.internalFacet = (InternalFacet)facetProcessors.processor(f.getType()).reduce(f.getName(), me.facets);
        	ret.entriesAsList.add(e);
        }
        	
        
        return ret;
    }

    static final class Fields {
        static final XContentBuilderString _TYPE = new XContentBuilderString("_type");
        static final XContentBuilderString ENTRIES = new XContentBuilderString("entries");
        static final XContentBuilderString TIME = new XContentBuilderString("time");
    }

    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(name);
        builder.field(Fields._TYPE, TYPE);
        builder.startArray(Fields.ENTRIES);
        for (Entry entry : collapseToAList()) {
            builder.startObject();
            builder.field(Fields.TIME, entry.time);
            entry.internalFacet.toXContent(builder,params);
            builder.endObject();
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }

    public static FacetedDateHistogramFacet readFacetedHistogramFacet(StreamInput in) throws IOException {
    	FacetedDateHistogramFacet facet = new FacetedDateHistogramFacet();
        facet.readFrom(in);
        return facet;
    }

    public void readFrom(StreamInput in) throws IOException {
        name = in.readUTF();

        int size = in.readVInt();
        entries = CacheRecycler.popLongObjectMap();
        for (int i = 0; i < size; i++) {
        	Entry e = new Entry(in.readLong(),null);
        	
        	String internal_type = in.readUTF();
        	InternalFacet facet = (InternalFacet)InternalFacet.Streams.stream(internal_type).readFacet(internal_type, in);
        	e.internalFacet = facet;
            entries.put(e.time,e);
        }
    }

    public void writeTo(StreamOutput out) throws IOException {
        out.writeUTF(name);
        out.writeVInt(entries.size());
        for (Entry e : collapseToAList()) {
            out.writeLong(e.time);
            out.writeUTF(e.internalFacet.streamType());
            e.internalFacet.writeTo(out);
        }
        releaseEntries();
    }


	public String name() {
		return name;
	}


	public String getName() {
		return name();
	}


	public String type() {
		return TYPE;
	}


	public String getType() {
		return type();
	}



}