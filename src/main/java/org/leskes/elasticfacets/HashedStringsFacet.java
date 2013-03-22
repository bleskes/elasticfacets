package org.leskes.elasticfacets;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.index.IndexReader;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.ElasticSearchIllegalStateException;
import org.elasticsearch.common.CacheRecycler;
import org.elasticsearch.common.collect.BoundedTreeSet;
import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.io.FastStringReader;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.trove.iterator.TObjectIntIterator;
import org.elasticsearch.common.trove.map.hash.TObjectIntHashMap;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.search.facet.Facet;
import org.elasticsearch.search.facet.InternalFacet;
import org.elasticsearch.search.facet.terms.TermsFacet;
import org.elasticsearch.search.internal.SearchContext;
import org.leskes.elasticfacets.fields.HashedStringFieldType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/**
 *
 */
public class HashedStringsFacet implements TermsFacet {

   private static final String STREAM_TYPE = "hashed_string_facet";

   public static void registerStreams() {
      InternalFacet.Streams.registerStream(STREAM, STREAM_TYPE);
   }

   static InternalFacet.Stream STREAM = new InternalFacet.Stream() {
      public Facet readFacet(String type, StreamInput in) throws IOException {
         return readHashedStringsFacet(in);
      }
   };

   public String streamType() {
      return STREAM_TYPE;
   }

   public static final String TYPE = "hashed_terms";

   @SuppressWarnings("unchecked")
   @Override
   public Iterator<Entry> iterator() {
      return (Iterator)entries.iterator();
   }


   public static class HashedStringEntry implements TermsFacet.Entry {

      final static ESLogger logger = Loggers.getLogger(HashedStringEntry.class);

      private String term;
      private int termHash;
      private int count;
      private int docId;

      public HashedStringEntry(String term, int termHash, int docId, int count) {
         this.term = term;
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

      public String term() {
         return term;
      }

      @Override
      public String getTerm() {
         return term;
      }

      @Override
      public Number termAsNumber() {
         throw new RuntimeException("Not implemented");
      }

      @Override
      public Number getTermAsNumber() {
         throw new RuntimeException("Not implemented");
      }

      public int getTermHash() {
         return termHash;
      }

      @Override
      public int hashCode() {
         return termHash;
      }

      @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
      @Override
      public boolean equals(Object o) {
         return termHash == o.hashCode();
      }

      @Override
      public int compareTo(TermsFacet.Entry o) {
         HashedStringEntry oe = (HashedStringEntry)o;
         int i;
         if (term != null && oe.term != null)
             i = term.compareTo(oe.term);
         else
            i = termHash - oe.termHash;

         if (i == 0) {
            i = count - o.count();
            if (i == 0) {
               i = System.identityHashCode(this) - System.identityHashCode(o);
            }
         }
         return i;
      }

      public void setCount(int count) {
         this.count = count;
      }

      public void loadTermFromDoc(SearchContext context, String indexFieldName, Analyzer fieldIndexAnalyzer) {
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
                  term = candidate;
                  return;
               }
            }
         } else if (value != null) {
            candidate = analyzeStringForTerm(value.toString(), termHash, indexFieldName, fieldIndexAnalyzer);
            if (candidate != null) {
               term = candidate;
               return;
            }

         }
         throw new ElasticSearchIllegalStateException(
                 "Failed to find hash code " + termHash + " in an array of docId " + docId +
                         ". You can only use stored fields or when you store the original document under _source");
      }

      private String analyzeStringForTerm(String fieldValue, int termHash, String indexFieldName, Analyzer fieldIndexAnalyzer) {
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

      public int getDocId() {
         return docId;
      }

      /*
         Do this at your own risk. It is the caller responsibility to deal with potential miss match
         with hash code.
       */
      public void setTerm(String term) {
         this.term = term;
      }
   }


   String name;
   int requiredSize;
   long missing;
   long total;
   protected Collection<HashedStringEntry> entries;
   TermsFacet.ComparatorType comparatorType;


   private HashedStringsFacet() {
   }

   public HashedStringsFacet(String name, TermsFacet.ComparatorType comparatorType, int requiredSize,
                             Collection<HashedStringEntry> entries, long missing, long total) {
      this.name = name;
      this.comparatorType = comparatorType;
      this.requiredSize = requiredSize;
      this.entries = entries;
      this.missing = missing;
      this.total = total;
   }


   public Facet reduce(String name, List<Facet> facets) {
      if (facets.size() == 1) {
         return facets.get(0);
      }
      HashedStringsFacet first = (HashedStringsFacet) facets.get(0);
      TObjectIntHashMap<HashedStringEntry> aggregated = CacheRecycler.popObjectIntMap();
      long missing = 0;
      long total = 0;
      for (Facet facet : facets) {
         HashedStringsFacet mFacet = (HashedStringsFacet) facet;
         missing += mFacet.missingCount();
         total += mFacet.totalCount();
         for (HashedStringEntry entry : mFacet.entries) {
            aggregated.adjustOrPutValue(entry, entry.count(), entry.count());
         }
      }

      BoundedTreeSet<HashedStringEntry> ordered = new BoundedTreeSet<HashedStringEntry>(first.comparatorType.comparator(), first.requiredSize);
      for (TObjectIntIterator<HashedStringEntry> it = aggregated.iterator(); it.hasNext(); ) {
         it.advance();
         HashedStringEntry entry = it.key();
         entry.setCount(it.value());
         ordered.add(entry);
      }
      first.entries = ordered;
      first.missing = missing;
      first.total = total;

      CacheRecycler.pushObjectIntMap(aggregated);

      return first;
   }

   public long totalCount() {
      return total;
   }

   @Override
   public long getTotalCount() {
      return totalCount();
   }

   public long missingCount() {
      return missing;
   }

   @Override
   public long getMissingCount() {
      return missingCount();
   }

   public long otherCount() {
      long other = total;
      for (HashedStringEntry entry : entries) {
         other -= entry.count();
      }
      return other;
   }

   public long getOtherCount() {
      return otherCount();
   }

   public List<HashedStringEntry> entries() {
      if (!(entries instanceof List)) {
         entries = ImmutableList.copyOf(entries);
      }
      return (List<HashedStringEntry>) entries;
   }

   @Override
   public List<? extends Entry> getEntries() {
      return entries();
   }

   static final class Fields {
      static final XContentBuilderString _TYPE = new XContentBuilderString("_type");
      static final XContentBuilderString MISSING = new XContentBuilderString("missing");
      static final XContentBuilderString TOTAL = new XContentBuilderString("total");
      static final XContentBuilderString OTHER = new XContentBuilderString("other");
      static final XContentBuilderString TERMS = new XContentBuilderString("terms");
      static final XContentBuilderString TERM = new XContentBuilderString("term");
      static final XContentBuilderString HASH = new XContentBuilderString("hash");
      static final XContentBuilderString COUNT = new XContentBuilderString("count");
   }

   public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
      builder.startObject(name);
      builder.field(Fields._TYPE, TermsFacet.TYPE);
      builder.field(Fields.MISSING, missing);
      builder.field(Fields.TOTAL, total);
      builder.field(Fields.OTHER, otherCount());
      builder.startArray(Fields.TERMS);
      for (HashedStringEntry entry : entries) {
         builder.startObject();
         if (entry.term() != null)
            builder.field(Fields.TERM, entry.term());
         else
            builder.field(Fields.HASH, entry.getTermHash());
         builder.field(Fields.COUNT, entry.count());
         builder.endObject();
      }
      builder.endArray();
      builder.endObject();
      return builder;
   }

   public static HashedStringsFacet readHashedStringsFacet(StreamInput in) throws IOException {
      HashedStringsFacet facet = new HashedStringsFacet();
      facet.readFrom(in);
      return facet;
   }


   public void readFrom(StreamInput in) throws IOException {
      name = in.readString();
      comparatorType = TermsFacet.ComparatorType.fromId(in.readByte());
      requiredSize = in.readVInt();
      missing = in.readVLong();
      total = in.readVLong();

      int size = in.readVInt();
      entries = new ArrayList<HashedStringEntry>(size);
      for (int i = 0; i < size; i++) {
         entries.add(new HashedStringEntry(in.readString(), in.readVInt(), in.readVInt(), in.readVInt()));
      }
   }

   public void writeTo(StreamOutput out) throws IOException {
      out.writeString(name);
      out.writeByte(comparatorType.id());
      out.writeVInt(requiredSize);
      out.writeVLong(missing);
      out.writeVLong(total);

      out.writeVInt(entries.size());
      for (HashedStringEntry entry : entries) {
         out.writeString(entry.term);
         out.writeVInt(entry.termHash);
         out.writeVInt(entry.docId);
         out.writeVInt(entry.count);
      }
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