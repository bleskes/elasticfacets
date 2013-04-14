
package org.leskes.elasticfacets;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.util.PriorityQueue;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.common.collect.ImmutableSet;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.index.cache.field.data.FieldDataCache;
import org.elasticsearch.index.field.data.FieldData;
import org.elasticsearch.index.field.data.FieldDataType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.facet.AbstractFacetCollector;
import org.elasticsearch.search.facet.Facet;
import org.elasticsearch.search.facet.terms.TermsFacet;
import org.elasticsearch.search.facet.terms.support.EntryPriorityQueue;
import org.elasticsearch.search.internal.SearchContext;
import org.leskes.elasticfacets.fields.HashedStringFieldData;
import org.leskes.elasticfacets.fields.HashedStringFieldSettings;
import org.leskes.elasticfacets.fields.HashedStringFieldType;
import org.leskes.elasticfacets.utils.SizeSensitiveCacheRecycler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class HashedStringsFacetCollector extends AbstractFacetCollector {

   final static ESLogger logger = Loggers
           .getLogger(HashedStringsFacetCollector.class);


   private final FieldDataCache fieldDataCache;

   private final String indexFieldName;

   private Analyzer fieldIndexAnalyzer;

   private final TermsFacet.ComparatorType comparatorType;

   public static enum OUTPUT_MODE {
      TERM, HASH, SCRIPT;

      public static OUTPUT_MODE fromString(String type) {
         if ("term".equals(type)) {
            return TERM;
         } else if ("hash".equals(type)) {
            return HASH;
         } else if ("script".equals(type)) {
            return SCRIPT;
         }
         throw new ElasticSearchIllegalArgumentException("No type argument match for hashed string facet output mode [" + type + "]");
      }
   }

   private final OUTPUT_MODE output_mode;

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

   private final HashedStringFieldSettings.FieldTypeFactory hashedStringTypeFactory;

   long missing;
   long total;

   private final ImmutableSet<Integer> excluded;
   private final ImmutableSet<Integer> included;

   public HashedStringsFacetCollector(String facetName, String fieldName, int size, int fetch_size,
                                      TermsFacet.ComparatorType comparatorType, boolean allTerms,
                                      OUTPUT_MODE output_mode,
                                      ImmutableSet<Integer> included, ImmutableSet<Integer> excluded,
                                      String output_script, String output_scriptLang, SearchContext context,
                                      Map<String, Object> params, HashedStringFieldSettings.FieldTypeFactory loaderForField) {
      super(facetName);
      this.fieldDataCache = context.fieldDataCache();
      this.size = size;
      this.fetch_size = fetch_size;
      this.comparatorType = comparatorType;
      this.numberOfShards = context.numberOfShards();
      this.context = context;
      this.output_mode = output_mode;
      this.hashedStringTypeFactory = loaderForField;


      MapperService.SmartNameFieldMappers smartMappers = context.smartFieldMappers(fieldName);
      if (smartMappers == null || !smartMappers.hasMapper()) {
         throw new ElasticSearchIllegalArgumentException(
                 "Field [" + fieldName + "] doesn't have a type, can't run hashed string facet collector on it");
      }
      // add type filter if there is exact doc mapper associated with it
      if (smartMappers.explicitTypeInNameWithDocMapper()) {
         setFilter(context.filterCache().cache(smartMappers.docMapper().typeFilter()));
      }

      if (smartMappers.mapper().fieldDataType() != FieldDataType.DefaultTypes.STRING) {
         throw new ElasticSearchIllegalArgumentException(
                 "Field [" + fieldName + "] is not of string type, can't run hashed string facet collector on it");
      }

      if (TermsFacet.ComparatorType.TERM.id() == comparatorType.id()) {
         throw new ElasticSearchIllegalArgumentException("HashedStringsFacet doesn't support sorting by term.");

      }

      if (output_script != null) {
         this.output_script = context.scriptService().search(context.lookup(), output_scriptLang, output_script, params);
      } else {
         this.output_script = null;
      }


      this.indexFieldName = smartMappers.mapper().names().indexName();
      this.fieldDataType = smartMappers.mapper().fieldDataType();


      this.fieldIndexAnalyzer = smartMappers.mapper().indexAnalyzer();
      if (this.fieldIndexAnalyzer == null && smartMappers.docMapper() != null)
         this.fieldIndexAnalyzer = smartMappers.docMapper().indexAnalyzer();
      if (this.fieldIndexAnalyzer == null) this.fieldIndexAnalyzer = Lucene.STANDARD_ANALYZER;

      if (excluded == null || excluded.isEmpty()) {
         this.excluded = null;
      } else {
         this.excluded = excluded;
      }
      if (included == null || included.isEmpty()) {
         this.included = null;
      } else {
         this.included = included;
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
      fieldData = (HashedStringFieldData) fieldDataCache.cache(hashedStringTypeFactory.getTypeForField(indexFieldName),
              reader, indexFieldName);
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
               if (agg.currentCount != 0) {
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
               if (included != null && !included.contains(value)) {
                  continue;
               }
               HashedStringsFacet.HashedStringEntry entry = new HashedStringsFacet.HashedStringEntry(null, value, docId, count);
               ordered.insertWithOverflow(entry);
            }
         }
         HashedStringsFacet.HashedStringEntry[] list = new HashedStringsFacet.HashedStringEntry[ordered.size()];
         for (int i = ordered.size() - 1; i >= 0; i--) {
            HashedStringsFacet.HashedStringEntry entry = (HashedStringsFacet.HashedStringEntry) ordered.pop();
            loadTermIntoEntry(entry);
            list[i] = entry;


         }

         for (ReaderAggregator aggregator : aggregators) {
            aggregator.close();
         }

         return new HashedStringsFacet(facetName, comparatorType, size, Arrays.asList(list), missing, total);
      }

      // TODO
      throw new UnsupportedOperationException("Large facet sizes (> 5000) are not yet implemented by HashedStringsFacet");

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

   private void loadTermIntoEntry(HashedStringsFacet.HashedStringEntry hashedEntry) {
      switch (output_mode) {
         case HASH:
            break;
         case TERM:
            String term = HashedStringFieldData.findTermInDoc(hashedEntry.getTermHash(), hashedEntry.getDocId(),
                    indexFieldName, fieldIndexAnalyzer, context);
            hashedEntry.setTerm(term);
            if (logger.isTraceEnabled())
               logger.trace("Converted hash entry: term={}, expected_hash={},real_hash={}, count={}, docId={}",
                       hashedEntry.term(), hashedEntry.getTermHash(), HashedStringFieldType.hashCode(hashedEntry.term()),
                       hashedEntry.count(), hashedEntry.getDocId());

            break;
         case SCRIPT:
            if (output_script == null)
               throw new ElasticSearchIllegalArgumentException(
                       "Hashed string facet output was set to script, but no script was supplied");
            int readerIndex = context.searcher().readerIndex(hashedEntry.getDocId());
            IndexReader subReader = context.searcher().subReaders()[readerIndex];
            int subDoc = hashedEntry.getDocId() - context.searcher().docStarts()[readerIndex];
            output_script.setNextReader(subReader);
            output_script.setNextDocId(subDoc);
            output_script.setNextVar("_hash",hashedEntry.hashCode());

            Object value;
            value = output_script.run();
            value = output_script.unwrap(value);
            hashedEntry.setTerm((String) value);
            break;
      }
   }

   public static class ReaderAggregator implements FieldData.OrdinalInDocProc {

      final int[] values;
      final int[] counts;
      final int[] docIdsForValues; // of every value keep a docid where we run into it.

      int position = 0; // first value is a null value.
      int currentValue;
      int currentDocId;
      int currentCount;
      int total;
      int missing;
      int docBase;

      public ReaderAggregator(HashedStringFieldData fieldData, int docBase) {
         this.values = fieldData.values();
         this.counts = SizeSensitiveCacheRecycler.popIntArray(fieldData.values().length);
         this.docIdsForValues = SizeSensitiveCacheRecycler.popIntArray(fieldData.values().length);
         this.docBase = docBase;
      }

      public void close() {
         SizeSensitiveCacheRecycler.pushIntArray(counts);
         SizeSensitiveCacheRecycler.pushIntArray(docIdsForValues);
      }

      public void onOrdinal(int docId, int ordinal) {
         if (ordinal == 0) {
            missing++;
            return; // no value no count..
         }
         if (counts[ordinal]++ == 0)
            docIdsForValues[ordinal] = docId + docBase;
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


   public static class AggregatorPriorityQueue extends PriorityQueue<ReaderAggregator> {

      public AggregatorPriorityQueue(int size) {
         initialize(size);
      }

      @Override
      protected boolean lessThan(ReaderAggregator a, ReaderAggregator b) {
         return a.currentValue < b.currentValue;
      }
   }
}
