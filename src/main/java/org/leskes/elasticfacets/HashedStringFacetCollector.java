
package org.leskes.elasticfacets;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.util.PriorityQueue;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.ElasticSearchIllegalStateException;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeResponse;
import org.elasticsearch.common.CacheRecycler;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.collect.BoundedTreeSet;
import org.elasticsearch.common.collect.ImmutableSet;
import org.elasticsearch.common.io.FastStringReader;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.document.ResetFieldSelector;
import org.elasticsearch.index.cache.field.data.FieldDataCache;
import org.elasticsearch.index.field.data.FieldData;
import org.elasticsearch.index.field.data.FieldDataType;
import org.elasticsearch.index.field.data.strings.StringFieldData;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.script.SearchScript;
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
import org.leskes.elasticfacets.fields.HashedStringFieldData;
import org.leskes.elasticfacets.fields.HashedStringFieldType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 */
public class HashedStringFacetCollector extends AbstractFacetCollector {

	final static ESLogger logger = Loggers
			.getLogger(HashedStringFacetCollector.class);

	
    private final FieldDataCache fieldDataCache;

    private final String indexFieldName;

    private Analyzer fieldIndexAnalyzer;

    private final TermsFacet.ComparatorType comparatorType;

    private final int size;

    private int fetch_size;

    private final int numberOfShards;

    private final int minCount;

    private final FieldDataType fieldDataType;

    private HashedStringFieldData fieldData;

    private final List<ReaderAggregator> aggregators;

    private ReaderAggregator current;
    
    private final SearchContext context;
    
    private final SearchScript output_script;

    long missing;
    long total;

    private final ImmutableSet<Integer> excluded;


	


    public HashedStringFacetCollector(String facetName, String fieldName, int size,int fetch_size,TermsFacet.ComparatorType comparatorType, boolean allTerms, 
    								  ImmutableSet<Integer> excluded,String output_script, String output_scriptLang, SearchContext context, 
    								  Map<String, Object> params) {
        super(facetName);
        this.fieldDataCache = context.fieldDataCache();
        this.size = size;
        this.fetch_size = fetch_size;
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
        
        if (output_script != null) {
            this.output_script = context.scriptService().search(context.lookup(), output_scriptLang, output_script, params);
        } else {
            this.output_script = null;
        }


        this.indexFieldName = smartMappers.mapper().names().indexName();
        this.fieldDataType = smartMappers.mapper().fieldDataType();
        
        
        this.fieldIndexAnalyzer = smartMappers.mapper().indexAnalyzer();
        if (this.fieldIndexAnalyzer == null &&  smartMappers.docMapper() != null) this.fieldIndexAnalyzer = smartMappers.docMapper().indexAnalyzer() ;
        if (this.fieldIndexAnalyzer == null ) this.fieldIndexAnalyzer = Lucene.STANDARD_ANALYZER;

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
            missing += current.missing;
            total += current.total;
            if (current.values.length > 0) {
                aggregators.add(current);
            }
        }
        fieldData = (HashedStringFieldData) fieldDataCache.cache(HashedStringFieldData.HASHED_STRING, reader, indexFieldName);
        current = new ReaderAggregator(fieldData, docBase);
    }

    @Override
    protected void doCollect(int doc) throws IOException {
        fieldData.forEachOrdinalInDoc(doc, current);
    }
    

    @Override
    public Facet facet() {
        if (current != null) {
            missing += current.missing;
            total += current.total;
            // if we have values for this one, add it
            if (current.values.length > 0) {
                aggregators.add(current);
            }
        }

        AggregatorPriorityQueue queue = new AggregatorPriorityQueue(aggregators.size());

        for (ReaderAggregator aggregator : aggregators) {
            if (aggregator.nextPosition()) {
                queue.add(aggregator);
            }
        }

        // if there is one shard, there will not be a reduce phase, so we must not deliver too much
        int queue_size = numberOfShards == 1 ? size : fetch_size; 

        // YACK, we repeat the same logic, but once with an optimizer priority queue for smaller sizes
        if (queue_size < EntryPriorityQueue.LIMIT) {
            // optimize to use priority size
            EntryPriorityQueue ordered = new EntryPriorityQueue(queue_size, comparatorType.comparator());

            while (queue.size() > 0) {
                ReaderAggregator agg = queue.top();
                int value = agg.currentValue;
                int count = 0;
                int docId = agg.currentDocId;
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
                
                assert (agg == null || value < agg.currentValue);

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
        throw new UnsupportedOperationException("Large facet sizes (> 5000) are not yet implemented by HashedStringFacet");

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
    	String term = getTermFromDoc(hashedEntry.docId,hashedEntry.termHash);
    	if (logger.isTraceEnabled())
    		logger.trace("Converted hash entry: term={}, expected_hash={},real_hash={}, count={}, docId={}",
    				term,hashedEntry.termHash,HashedStringFieldType.hashCode(term),hashedEntry.count(),hashedEntry.docId);
    	return new StringEntry(term, hashedEntry.count());
		
	}
    
    
    // Stolen for FetchPhase execute. Watch out for changes there
    private String getTermFromDoc(int docId,int termHash) {
    	int readerIndex = context.searcher().readerIndex(docId);
        IndexReader subReader = context.searcher().subReaders()[readerIndex];
        int subDoc = docId - context.searcher().docStarts()[readerIndex];
        if (output_script == null) {
            context.lookup().setNextReader(subReader);
            context.lookup().setNextDocId(subDoc);
            String candidate;
        	Object value = context.lookup().source().extractValue(this.indexFieldName);
        	if (value instanceof ArrayList<?>) {
        		for (Object v : (ArrayList<?>)value) {
        			candidate = analyzeStringForTerm(v.toString(),termHash);
        			if (candidate != null)
        				return candidate;
        		}
        	}
        	else {
        		candidate =  analyzeStringForTerm(value.toString(),termHash);
    			if (candidate != null)
    				return candidate;
        	}
        	
    		throw new ElasticSearchIllegalStateException("Failed to find hash code "+termHash+" in an array of docId "+docId);
        }
        else {
        	output_script.setNextReader(subReader);
        	output_script.setNextDocId(subDoc);

            Object value;
            value = output_script.run();
            value = output_script.unwrap(value);
            return value.toString();
        }
    }
    
    private String analyzeStringForTerm(String fieldValue,int termHash) {
        TokenStream stream = null;
        if (HashedStringFieldType.hashCode(fieldValue) == termHash) 
        	return fieldValue; // you never know :)
        
        String ret = null;
        try {
            stream = fieldIndexAnalyzer.reusableTokenStream(indexFieldName, new FastStringReader(fieldValue));
            stream.reset();
            CharTermAttribute term = stream.addAttribute(CharTermAttribute.class);
//            PositionIncrementAttribute posIncr = stream.addAttribute(PositionIncrementAttribute.class);
//            OffsetAttribute offset = stream.addAttribute(OffsetAttribute.class);
//            TypeAttribute type = stream.addAttribute(TypeAttribute.class);

            while (stream.incrementToken()) {
                ret = term.toString();
                logger.trace("Considering {} for hash code {}",ret,termHash);
                if (HashedStringFieldType.hashCode(ret) == termHash) {
                	logger.trace("Matched!");
                	break;
                }
            	ret = null;
            }
            stream.end();
        } catch (IOException e) {
            throw new ElasticSearchException("failed to analyze", e);
        } finally {
            if (stream != null) {
                try {
                    stream.close();
                } catch (IOException e) {
                    // ignore
                }
            }
        }
        return ret;
    }
   

	public static class ReaderAggregator implements FieldData.OrdinalInDocProc {

        final int[] values;
        final int[] counts;
        final int[] docIdsForValues; // of every value keep a docid where we run into it.

        int position = -1;
        int currentValue;
        int currentDocId;
        int currentCount;
        int total;
        int missing;
        int docBase;

        public ReaderAggregator(HashedStringFieldData fieldData,int docBase) {
            this.values = fieldData.values();
            this.counts = CacheRecycler.popIntArray(fieldData.values().length);
            this.docIdsForValues = CacheRecycler.popIntArray(fieldData.values().length);
            this.docBase = docBase;
        }

        public void close() {
			CacheRecycler.pushIntArray(counts);
			CacheRecycler.pushIntArray(docIdsForValues);
		}

		public void onOrdinal(int docId, int ordinal) {
			if (ordinal<0) {
				missing++;
				return; // no value no count..
			}
            counts[ordinal]++;
            docIdsForValues[ordinal] = docId+docBase;
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
