package org.leskes.elasticfacets.fields;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.index.TermEnum;
import org.apache.lucene.util.StringHelper;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.index.field.data.FieldData;

import java.io.IOException;

public class CompactFieldDataLoader {

   final static ESLogger logger = Loggers.getLogger(CompactFieldDataLoader.class);

   static int count = 0;

   @SuppressWarnings({"StringEquality"})
   public static <T extends FieldData> T load(final IndexReader reader, String field, final TypeLoader<T> loader) throws IOException {

      loader.init();

      field = StringHelper.intern(field);
      final int[] docTermCounts = new int[reader.maxDoc()];
      final boolean[] multiValued = {false};


      // first sweep, fill in docTermCounts

      readTermsAndDocs(reader, field, new TermsAndDocsProcessor() {
         @Override
         public void startTerm(String term) {
         }

         @Override
         public void addDoc(int doc) {
            if (docTermCounts[doc] > 0) multiValued[0] = true;
            docTermCounts[doc]++;
         }
      });

      logger.debug("Field {} initial scan done. Proclaimed {}",field, multiValued[0] ? "multi_valued" : "single_valued");

      if (multiValued[0]) {
         MultiValueOrdinalArray ordinalsArray = new MultiValueOrdinalArray(docTermCounts);
         final MultiValueOrdinalArray.OrdinalLoader ordinalLoader = ordinalsArray.createLoader();

         readTermsAndDocs(reader, field, new TermsAndDocsProcessor() {
            int t=0;

            @Override
            public void startTerm(String term) {
               t++;
               loader.collectTerm(term);
            }

            @Override
            public void addDoc(int doc) {
               ordinalLoader.addDocOrdinal(doc,t);
            }
         });

         return loader.buildMultiValue(field, ordinalsArray);


      }
      else {
         final int [] ordinals = new int[reader.maxDoc()];
         readTermsAndDocs(reader, field, new TermsAndDocsProcessor() {

            int t=0;
            @Override
            public void startTerm(String term) {
               t++;
               loader.collectTerm(term);
            }

            @Override
            public void addDoc(int doc) {
               ordinals[doc]=t;
            }
         });
         return loader.buildSingleValue(field,ordinals);
      }
   }

   private static interface TermsAndDocsProcessor {

      void startTerm(String term);
      void addDoc(int doc);
   }

   private static void readTermsAndDocs(IndexReader reader, String field, TermsAndDocsProcessor processor) throws IOException {
      TermDocs termDocs = reader.termDocs();
      TermEnum termEnum = reader.terms(new Term(field));

      // bulk read (in lucene 4 it won't be needed).
      int size = Math.min(128, reader.maxDoc());
      final int[] docs = new int[size];
      final int[] freqs = new int[size];

      try {
         do {
            Term term = termEnum.term();
            if (term == null || term.field() != field) break;
            processor.startTerm(term.text());
            termDocs.seek(termEnum);
            int number = termDocs.read(docs, freqs);
            while (number > 0) {
               for (int i = 0; i < number; i++) {
                  processor.addDoc(docs[i]);
               }
               number = termDocs.read(docs, freqs);
            }
         } while (termEnum.next());
      } catch (RuntimeException e) {
         if (e.getClass().getName().endsWith("StopFillCacheException")) {
            // all is well, in case numeric parsers are used.
         } else {
            throw e;
         }
      } finally {
         termDocs.close();
         termEnum.close();
      }
   }

   public static interface TypeLoader<T extends FieldData> {

      void init();

      void collectTerm(String term);

      T buildSingleValue(String fieldName, int[] ordinals);

      T buildMultiValue(String fieldName, MultiValueOrdinalArray ordinalsArray);
   }

   public static abstract class FreqsTypeLoader<T extends FieldData> implements TypeLoader<T> {

      protected FreqsTypeLoader() {
      }

      @Override
      public void init() {
      }
   }

}
