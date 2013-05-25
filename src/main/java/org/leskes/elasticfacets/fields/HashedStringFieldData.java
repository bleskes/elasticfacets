package org.leskes.elasticfacets.fields;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.util.OpenBitSet;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.ElasticSearchIllegalStateException;
import org.elasticsearch.common.RamUsage;
import org.elasticsearch.common.io.FastStringReader;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.trove.list.array.TIntArrayList;
import org.elasticsearch.common.trove.set.hash.TIntHashSet;
import org.elasticsearch.index.field.data.FieldData;
import org.elasticsearch.index.field.data.FieldDataType;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 */
public abstract class HashedStringFieldData extends FieldData<HashedStringDocFieldData> {

   public static final HashedStringFieldType HASHED_STRING_UNLOADABLE = new HashedStringFieldType(null);

   protected static final ESLogger logger = Loggers.getLogger(HashedStringFieldData.class);

   protected final int[] values;

   protected int collisions;

   protected HashedStringFieldData(String fieldName, int[] values) {
      super(fieldName);
      this.values = values;
      if (values.length == 0)
         collisions = 0;
      else {
         int prv = values[0];
         for (int i = 1; i < values.length; i++) {
            if (values[i] == prv) collisions++;
            prv = values[i];
         }
      }

      if (collisions > 0)
         logger.warn("HashedStringFieldData initialized, but with {} collisions. Total value count: {}", collisions, values.length);
   }

   public int[] values() {
      return values;
   }

   public int collisions() {
      return collisions;
   }


   @Override
   protected long computeSizeInBytes() {
      long size = RamUsage.NUM_BYTES_ARRAY_HEADER;
      size += values.length * RamUsage.NUM_BYTES_INT;
      return size;
   }


   @Override
   public HashedStringDocFieldData docFieldData(int docId) {
      return super.docFieldData(docId);
   }


   @Override
   public void forEachValue(StringValueProc proc) {
      throw new UnsupportedOperationException("HashedStringData doesn't support string iteration. Those are gone");
   }

   @Override
   public void forEachValueInDoc(
           int docId, StringValueInDocProc proc) {
      throw new UnsupportedOperationException("HashedStringData doesn't support string iteration. Those are gone");

   }

   abstract public void forEachValueInDoc(int docId, HashedStringValueInDocProc proc);


   @Override
   public String stringValue(int docId) {
      throw new UnsupportedOperationException("Hashed string field data destroyes original valus");
   }

   @Override
   protected HashedStringDocFieldData createFieldData() {
      return new HashedStringDocFieldData(this);
   }

   @Override
   public FieldDataType type() {
      return HashedStringFieldData.HASHED_STRING_UNLOADABLE;
   }

   public static interface HashedStringValueInDocProc {
      void onValue(int docId, int Hash);

      void onMissing(int docId);
   }


   public static class HashedStringTypeLoader implements MultiSweepFieldDataLoader.TypeLoader<HashedStringFieldData> {

      private final TIntArrayList hashed_terms = new TIntArrayList();

      private int[] sorted_hashed_terms;
      private int[] new_location_of_hashed_terms_in_sorted;

      final int max_terms_per_doc;
      final int min_docs_per_term;
      final Matcher excludeMatcher;
      final TIntHashSet excludeTerms;

      OrdinalLoader ordinalLoader = null;
      int[] docTermsCounts;
      boolean multiValued = false;
      final OpenBitSet skippedTermsCache = new OpenBitSet(1000);
      int termsSkipped;
      long stringTermsBytes;
      int currentTerm;
      int currentOrdinal; // if terms are rejected, ordinal is not upgraded.
      boolean initialSweep;
      String field;

      public HashedStringTypeLoader(int max_terms_per_doc, int min_docs_per_term, Pattern excludePattern,
                                    TIntHashSet excludeTerms) {
         super();
         this.max_terms_per_doc = max_terms_per_doc;
         this.min_docs_per_term = min_docs_per_term;
         this.excludeMatcher = excludePattern != null? excludePattern.matcher(""): null;
         this.excludeTerms = excludeTerms;
         // the first one indicates null value.
         hashed_terms.add(0);

      }

      @Override
      public void init(String field, int docCount) {
         docTermsCounts = new int[docCount];
         currentTerm = -1;
         currentOrdinal = 0; // first ordinal is 1
         termsSkipped=0;
         initialSweep = true;
         this.field = field;

         logger.debug("Loading field {}, max_terms_per_doc={} min_docs_per_term={},excludeTerms#={}, excludePattern={}",
                 field, max_terms_per_doc, min_docs_per_term, excludeTerms==null?0:excludeTerms.size(),
                 excludeMatcher==null?"":excludeMatcher.pattern().pattern());

      }

      @Override
      public boolean finalizeSweep() {
         if (!initialSweep) return false;
         initialSweep = false;
         int docsSkipped = 0;

         if (multiValued) {
            if (max_terms_per_doc > 0) {
               logger.debug("resetting doc with too many terms");
               for (int i = 0; i < docTermsCounts.length; i++) {
                  if (docTermsCounts[i] > max_terms_per_doc) {
                     docTermsCounts[i] = 0; // reset.
                     docsSkipped++;
                  }
               }

            }
            MultiValueOrdinalArray ordinalsArray = new MultiValueOrdinalArray(docTermsCounts);
            ordinalLoader = ordinalsArray.createLoader();
         }
         else {
            ordinalLoader = new SingleValueOrdinalLoader(docTermsCounts.length);
         }

         logger.debug("Field {} initial scan done. {} terms (original byte size {}, {} terms skipped). {} docs ({} skipped) Proclaimed {}.",
                 field, currentTerm+1, stringTermsBytes, termsSkipped, docTermsCounts.length, docsSkipped,
                 multiValued ? "multi_valued" : "single_valued");

         currentTerm = -1;
         currentOrdinal = 0;

         return true;
      }

      private boolean shouldSkipTerm(String term, int termDocCount) {
         if  (min_docs_per_term > 0 && termDocCount < min_docs_per_term) return true;
         if (excludeTerms != null && excludeTerms.contains(HashedStringFieldType.hashCode(term))) {
            return true;
         }
         if (excludeMatcher != null)  {
            excludeMatcher.reset(term);
            if (excludeMatcher.matches()) return true;
         }
         return false;
      }

      public MultiSweepFieldDataLoader.TERM_STATE collectTerm(String term, int termDocCount) {
         currentTerm++;
         if (initialSweep) {
             stringTermsBytes += term.length()*2+40;
            // only check skipping and cache it for the next round.
            boolean skip = shouldSkipTerm(term, termDocCount);
            if (skip) {
               termsSkipped++;
               skippedTermsCache.set(currentTerm);
               return MultiSweepFieldDataLoader.TERM_STATE.SKIP;
            }
            return MultiSweepFieldDataLoader.TERM_STATE.PROCESS;
         }
         // second sweep
         if (skippedTermsCache.get(currentTerm))
            return MultiSweepFieldDataLoader.TERM_STATE.SKIP;

         hashed_terms.add(HashedStringFieldType.hashCode(term));
         currentOrdinal++;
         return MultiSweepFieldDataLoader.TERM_STATE.PROCESS;
      }

      @Override
      public void addTermDoc(int doc) {
         if (initialSweep) {
            if (++docTermsCounts[doc] > 1) multiValued = true;
         } else {
            if (docTermsCounts[doc] >0) // 0 marks skipping
               ordinalLoader.addDocOrdinal(doc, currentOrdinal);
         }
      }

      @Override
      public HashedStringFieldData buildFieldData() {
         if (multiValued) {
            MultiValueOrdinalArray array = ((MultiValueOrdinalArray.MultiValueOrdinalLoader) ordinalLoader).getArray();
            return buildMultiValue(array);
         } else {
            int[] array = ((SingleValueOrdinalLoader) ordinalLoader).getOrdinals();
            return buildSingleValue(array);
         }
      }

      protected void sort_values() {
         // as we hashed the values they are not sorted. They need to be for proper working of the rest.
         Integer[] translation_indices = new Integer[hashed_terms.size() - 1]; // drop the first "non value place"
         for (int i = 0; i < translation_indices.length; i++)
            translation_indices[i] = i + 1; // one offset for the dropped place
         Arrays.sort(translation_indices, new Comparator<Integer>() {

            public int compare(Integer paramT1, Integer paramT2) {
               int d1 = hashed_terms.get(paramT1);
               int d2 = hashed_terms.get(paramT2);
               return d1 < d2 ? -1 : (d1 == d2 ? 0 : 1);
            }
         }
         );


         // now build a sorted array and update the ordinal values (added the n value in the beginning)
         sorted_hashed_terms = new int[hashed_terms.size()];

         new_location_of_hashed_terms_in_sorted = new int[hashed_terms.size()];
         for (int i = 1; i <= translation_indices.length; i++) {
            sorted_hashed_terms[i] = hashed_terms.get(translation_indices[i - 1]);
            new_location_of_hashed_terms_in_sorted[translation_indices[i - 1]] = i;
         }


      }

      protected void updateOrdinalArray(int[] ordinals) {
         for (int i = 0; i < ordinals.length; i++)
            ordinals[i] = new_location_of_hashed_terms_in_sorted[ordinals[i]];
      }


      public HashedStringFieldData buildSingleValue(int[] ordinals) {

         sort_values();

         updateOrdinalArray(ordinals);

         return new SingleValueHashedStringFieldData(field, sorted_hashed_terms, ordinals);
      }


      public HashedStringFieldData buildMultiValue(MultiValueOrdinalArray ordinalsArray) {
         sort_values();

         // we need to do translation, count again...

         int[] docOrdinalCount = new int[ordinalsArray.maxDoc()];
         for (int docId = 0; docId < docOrdinalCount.length; docId++) {
            MultiValueOrdinalArray.OrdinalIterator ordIterator = ordinalsArray.getOrdinalIteratorForDoc(docId);
            while (ordIterator.getNextOrdinal() != 0) docOrdinalCount[docId]++;
         }

         MultiValueOrdinalArray translatedOrdinals = new MultiValueOrdinalArray(docOrdinalCount);
         MultiValueOrdinalArray.MultiValueOrdinalLoader ordLoader = translatedOrdinals.createLoader();
         for (int docId = 0; docId < docOrdinalCount.length; docId++) {
            MultiValueOrdinalArray.OrdinalIterator ordIterator = ordinalsArray.getOrdinalIteratorForDoc(docId);
            int o = ordIterator.getNextOrdinal();
            while (o != 0) {
               ordLoader.addDocOrdinal(docId, new_location_of_hashed_terms_in_sorted[o]);
               o = ordIterator.getNextOrdinal();
            }
         }

         return new MultiValueHashedStringFieldData(field, sorted_hashed_terms, translatedOrdinals);
      }
   }

   public static String findTermInDoc(int termHash, int docId, String indexFieldName, Analyzer fieldIndexAnalyzer,
                                      SearchContext context) {
      int readerIndex = context.searcher().readerIndex(docId);
      IndexReader subReader = context.searcher().subReaders()[readerIndex];
      int subDoc = docId - context.searcher().docStarts()[readerIndex];
      context.lookup().setNextReader(subReader);
      context.lookup().setNextDocId(subDoc);
      String candidate;
      Object value = context.lookup().source().extractValue(indexFieldName);
      if (value instanceof ArrayList<?>) {
         for (Object v : (ArrayList<?>) value) {
            if (v == null) continue;
            candidate = analyzeStringForTerm(v.toString(), termHash, indexFieldName, fieldIndexAnalyzer);
            if (candidate != null) {
               return candidate;
            }
         }
      } else if (value != null) {
         candidate = analyzeStringForTerm(value.toString(), termHash, indexFieldName, fieldIndexAnalyzer);
         if (candidate != null) {
            return candidate;
         }
      }
      throw new ElasticSearchIllegalStateException(
              "Failed to find hash code " + termHash + " in an array of docId " + docId +
                      ". You can only use stored fields or when you store the original document under _source");
   }

   public static String analyzeStringForTerm(String fieldValue, int termHash, String indexFieldName, Analyzer fieldIndexAnalyzer) {
      TokenStream stream = null;
      if (HashedStringFieldType.hashCode(fieldValue) == termHash)
         return fieldValue; // you never know :)

      String ret = null;
      try {
         stream = fieldIndexAnalyzer.reusableTokenStream(indexFieldName, new FastStringReader(fieldValue));
         stream.reset();
         CharTermAttribute term = stream.addAttribute(CharTermAttribute.class);

         while (stream.incrementToken()) {
            ret = term.toString();
            logger.trace("Considering {} for hash code {}", ret, termHash);
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


}
