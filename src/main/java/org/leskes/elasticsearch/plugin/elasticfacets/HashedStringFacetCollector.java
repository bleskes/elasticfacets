
package org.leskes.elasticsearch.plugin.elasticfacets;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.util.PriorityQueue;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.common.CacheRecycler;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.collect.BoundedTreeSet;
import org.elasticsearch.common.collect.ImmutableSet;
import org.elasticsearch.common.lucene.document.ResetFieldSelector;
import org.elasticsearch.index.cache.field.data.FieldDataCache;
import org.elasticsearch.index.field.data.FieldData;
import org.elasticsearch.index.field.data.FieldDataType;
import org.elasticsearch.index.field.data.strings.StringFieldData;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.facet.AbstractFacetCollector;
import org.elasticsearch.search.facet.Facet;
import org.elasticsearch.search.facet.terms.TermsFacet;
import org.elasticsearch.search.facet.terms.TermsFacet.Entry;
import org.elasticsearch.search.facet.terms.strings.InternalStringTermsFacet;
import org.elasticsearch.search.facet.terms.strings.InternalStringTermsFacet.StringEntry;
import org.elasticsearch.search.facet.terms.support.EntryPriorityQueue;
import org.elasticsearch.search.fetch.FetchPhaseExecutionException;
import org.elasticsearch.search.internal.InternalSearchHitField;
import org.elasticsearch.search.internal.SearchContext;
import org.leskes.elasticsearch.plugin.elasticfacets.fields.HashedStringFieldData;
import org.leskes.elasticsearch.plugin.elasticfacets.fields.HashedStringFieldType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 */
public class HashedStringFacetCollector extends AbstractFacetCollector {

    private final FieldDataCache fieldDataCache;

    private final String indexFieldName;

    private final TermsFacet.ComparatorType comparatorType;

    private final int size;

    private final int numberOfShards;

    private final int minCount;

    private final FieldDataType fieldDataType;

    private HashedStringFieldData fieldData;

    private final List<ReaderAggregator> aggregators;

    private ReaderAggregator current;
    
    private final SearchContext context;

    long missing;
    long total;

    private final ImmutableSet<Integer> excluded;

    public HashedStringFacetCollector(String facetName, String fieldName, int size, TermsFacet.ComparatorType comparatorType, boolean allTerms, 
    								  ImmutableSet<Integer> excluded,SearchContext context) {
        super(facetName);
        this.fieldDataCache = context.fieldDataCache();
        this.size = size;
        this.comparatorType = comparatorType;
        this.numberOfShards = context.numberOfShards();
        this.context = context;

        MapperService.SmartNameFieldMappers smartMappers = context.smartFieldMappers(fieldName);
        if (smartMappers == null || !smartMappers.hasMapper()) {
            throw new ElasticSearchIllegalArgumentException("Field [" + fieldName + "] doesn't have a type, can't run hashed string facet collector on it");
        }
        // add type filter if there is exact doc mapper associated with it
        if (smartMappers.explicitTypeInNameWithDocMapper()) {
            setFilter(context.filterCache().cache(smartMappers.docMapper().typeFilter()));
        }

        if (smartMappers.mapper().fieldDataType() != FieldDataType.DefaultTypes.STRING) {
            throw new ElasticSearchIllegalArgumentException("Field [" + fieldName + "] is not of string type, can't run hashed string facet collector on it");
        }
        
        if (TermsFacet.ComparatorType.TERM.id() == comparatorType.id()) {
            throw new ElasticSearchIllegalArgumentException("HashedStringFacet doesn't support sorting by term.");
        	
        }

        this.indexFieldName = smartMappers.mapper().names().indexName();
        this.fieldDataType = smartMappers.mapper().fieldDataType();

        if (excluded == null || excluded.isEmpty()) {
            this.excluded = null;
        } else {
            this.excluded = excluded;
        }

        // minCount is offset by -1
        if (allTerms) {
            minCount = -1;
        } else {
            minCount = 0;
        }

        this.aggregators = new ArrayList<ReaderAggregator>(context.searcher().subReaders().length);
    }

    @Override
    protected void doSetNextReader(IndexReader reader, int docBase) throws IOException {
        if (current != null) {
            missing += current.counts[0];
            total += current.total - current.counts[0];
            if (current.values.length > 1) {
                aggregators.add(current);
            }
        }
        fieldData = (HashedStringFieldData) fieldDataCache.cache(HashedStringFieldData.HASHED_STRING, reader, indexFieldName);
        current = new ReaderAggregator(fieldData);
    }

    @Override
    protected void doCollect(int doc) throws IOException {
        fieldData.forEachOrdinalInDoc(doc, current);
    }
    

    @Override
    public Facet facet() {
        if (current != null) {
            missing += current.counts[0];
            total += current.total - current.counts[0];
            // if we have values for this one, add it
            if (current.values.length > 1) {
                aggregators.add(current);
            }
        }

        AggregatorPriorityQueue queue = new AggregatorPriorityQueue(aggregators.size());

        for (ReaderAggregator aggregator : aggregators) {
            if (aggregator.nextPosition()) {
                queue.add(aggregator);
            }
        }

        // YACK, we repeat the same logic, but once with an optimizer priority queue for smaller sizes
        if (size < EntryPriorityQueue.LIMIT) {
            // optimize to use priority size
            EntryPriorityQueue ordered = new EntryPriorityQueue(size, comparatorType.comparator());

            while (queue.size() > 0) {
                ReaderAggregator agg = queue.top();
                int value = agg.currentValue;
                int count = 0;
                int docId = 0;
                do {
                	if (agg.currentCount != 0 ) {
                		count += agg.currentCount;
                		docId = agg.currentDocId;
                	}
                    
                    if (agg.nextPosition()) {
                        agg = queue.updateTop();
                    } else {
                        // we are done with this reader
                        queue.pop();
                        agg = queue.top();
                    }
                } while (agg != null && value == agg.currentValue);

                if (count > minCount) {
                    if (excluded != null && excluded.contains(value)) {
                        continue;
                    }
                    HashedStringEntry entry = new HashedStringEntry(value,docId, count);
                    ordered.insertWithOverflow(entry);
                }
            }
            InternalStringTermsFacet.StringEntry[] list = new InternalStringTermsFacet.StringEntry[ordered.size()];
            for (int i = ordered.size() - 1; i >= 0; i--) {
                list[i] = convertHashEntryToStringEntry((HashedStringEntry)ordered.pop());
            }

            for (ReaderAggregator aggregator : aggregators) {
            	aggregator.close();
            }

            return new InternalStringTermsFacet(facetName, comparatorType, size, Arrays.asList(list), missing, total);
        }
        
        // TODO
        throw new UnsupportedOperationException("Large facet sizes are not yet implemented by HashedStringFacet");

//        BoundedTreeSet<InternalStringTermsFacet.StringEntry> ordered = new BoundedTreeSet<InternalStringTermsFacet.StringEntry>(comparatorType.comparator(), size);
//
//        while (queue.size() > 0) {
//            ReaderAggregator agg = queue.top();
//            String value = agg.current;
//            int count = 0;
//            do {
//                count += agg.counts[agg.position];
//                if (agg.nextPosition()) {
//                    agg = queue.updateTop();
//                } else {
//                    // we are done with this reader
//                    queue.pop();
//                    agg = queue.top();
//                }
//            } while (agg != null && value.equals(agg.current));
//
//            if (count > minCount) {
//                if (excluded != null && excluded.contains(value)) {
//                    continue;
//                }
//                if (matcher != null && !matcher.reset(value).matches()) {
//                    continue;
//                }
//                InternalStringTermsFacet.StringEntry entry = new InternalStringTermsFacet.StringEntry(value, count);
//                ordered.add(entry);
//            }
//        }
//
//
//        for (ReaderAggregator aggregator : aggregators) {
//            CacheRecycler.pushIntArray(aggregator.counts);
//        }
//
//        return new InternalStringTermsFacet(facetName, comparatorType, size, ordered, missing, total);
    }

    private StringEntry convertHashEntryToStringEntry(HashedStringEntry hashedEntry) {
    	String term = getTermFromDoc(hashedEntry.docId);
    	return new StringEntry(term, hashedEntry.count());
		
	}
    
    
    // Stolen for FetchPhase execute. Watch out for changes there
    private String getTermFromDoc(int docId) {
    	int readerIndex = context.searcher().readerIndex(docId);
        IndexReader subReader = context.searcher().subReaders()[readerIndex];
        int subDoc = docId - context.searcher().docStarts()[readerIndex];
        context.lookup().setNextReader(subReader);
        context.lookup().setNextDocId(subDoc);
        return (String)context.lookup().source().extractValue(this.indexFieldName);
    }
   

	public static class ReaderAggregator implements FieldData.OrdinalInDocProc {

        final int[] values;
        final int[] counts;
        final int[] docIdsForValues; // of every value keep a docid where we run into it.

        int position = 0;
        int currentValue;
        int currentDocId;
        int currentCount;
        int total;

        public ReaderAggregator(HashedStringFieldData fieldData) {
            this.values = fieldData.values();
            this.counts = CacheRecycler.popIntArray(fieldData.values().length);
            this.docIdsForValues = CacheRecycler.popIntArray(fieldData.values().length);
        }

        public void close() {
			CacheRecycler.pushIntArray(counts);
			CacheRecycler.pushIntArray(docIdsForValues);
		}

		public void onOrdinal(int docId, int ordinal) {
            counts[ordinal]++;
            docIdsForValues[ordinal] = docId;
            total++;
        }

        public boolean nextPosition() {
            if (++position >= values.length) {
                return false;
            }
            currentValue = values[position];
            currentDocId = docIdsForValues[position];
            currentCount = counts[position];
            return true;
        }
    }
    
    public static class HashedStringEntry implements Entry {

        private int termHash;
        private int count;
        private int docId;

        public HashedStringEntry(int termHash, int docId, int count) {
            this.termHash = termHash;
            this.count = count;
            this.docId = docId;
        }

        public int count() {
            return count;
        }

        public int getCount() {
            return count();
        }

        public int compareTo(Entry o) {
        	HashedStringEntry ho = (HashedStringEntry) o;
            int i = termHash > ho.termHash ? +1 : termHash < ho.termHash ? -1 : 0; 
            if (i == 0) {
                i = count - o.count();
                if (i == 0) {
                    i = System.identityHashCode(this) - System.identityHashCode(o);
                }
            }
            return i;
        }
        
		public String term() {
			throw new UnsupportedOperationException();
		}

		public String getTerm() {
			throw new UnsupportedOperationException();
		}

		public Number termAsNumber() {
			throw new UnsupportedOperationException();
		}

		public Number getTermAsNumber() {
			throw new UnsupportedOperationException();
		}
    }

    public static class AggregatorPriorityQueue extends PriorityQueue<ReaderAggregator> {

        public AggregatorPriorityQueue(int size) {
            initialize(size);
        }

        @Override
        protected boolean lessThan(ReaderAggregator a, ReaderAggregator b) {
            return  a.currentValue < b.currentValue;
        }
    }
}
