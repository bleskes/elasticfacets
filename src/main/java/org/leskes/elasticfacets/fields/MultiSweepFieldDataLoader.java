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

public class MultiSweepFieldDataLoader {

   final static ESLogger logger = Loggers.getLogger(MultiSweepFieldDataLoader.class);

   @SuppressWarnings({"StringEquality"})
   public static <T extends FieldData> T load(final IndexReader reader, String field, final TypeLoader<T> loader)
           throws IOException {

      logger.info("Loading field {}", field);

      field = StringHelper.intern(field);
      loader.init(field, reader.maxDoc());

      do
      {
         readTermsAndDocs(reader,field,loader);
      }
      while(loader.finalizeSweep());

      return loader.buildFieldData();
   }

   enum TERM_STATE {
      PROCESS,
      SKIP,
      ABORT
   }


   private static <T extends FieldData> void readTermsAndDocs(IndexReader reader,
                                                              String field,
                                                              final TypeLoader<T> loader) throws IOException {
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
            TERM_STATE TS = loader.collectTerm(term.text(), termEnum.docFreq());
            if (TS == TERM_STATE.SKIP)
               continue;
            else if (TS == TERM_STATE.ABORT)
               break;

            termDocs.seek(termEnum);
            int number = termDocs.read(docs, freqs);
            while (number > 0) {
               for (int i = 0; i < number; i++) {
                  loader.addTermDoc(docs[i]);
               }
               number = termDocs.read(docs, freqs);
            }
         } while (termEnum.next());

      } finally {
         termDocs.close();
         termEnum.close();
      }
   }

   public static interface TypeLoader<T extends FieldData> {

      void init(String field, int docCount);

      // should return true if another sweep is required
      boolean finalizeSweep();

      TERM_STATE collectTerm(String term, int termDocCount);

      void addTermDoc(int doc);

      T buildFieldData();
   }


}
