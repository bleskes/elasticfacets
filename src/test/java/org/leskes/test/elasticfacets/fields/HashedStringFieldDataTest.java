package org.leskes.test.elasticfacets.fields;

import org.apache.lucene.analysis.miscellaneous.PatternAnalyzer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;
import org.elasticsearch.common.lucene.DocumentBuilder;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.field.data.FieldData.OrdinalInDocProc;
import org.leskes.elasticfacets.fields.HashedStringFieldData;
import org.leskes.elasticfacets.fields.HashedStringFieldType;
import org.leskes.elasticfacets.fields.MultiValueHashedStringFieldData;
import org.leskes.elasticfacets.fields.SingleValueHashedStringFieldData;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;

public class HashedStringFieldDataTest {
	protected void assertHash(String A, String B) {
		AssertJUnit.assertEquals("Hash code of " + A
				+ " doesn't equal the one of " + B,
				HashedStringFieldType.hashCode(A),
				HashedStringFieldType.hashCode(B));
	}

	protected void assertHash(int A, String B) {
		AssertJUnit.assertEquals("Hash code doesn't equal the one of " + B, A,
				HashedStringFieldType.hashCode(B));
	}

	protected void assertHash(String A, int B) {
		AssertJUnit.assertEquals("Hash code doesn't equal the one of " + A,
				HashedStringFieldType.hashCode(A), B);
	}

	protected void assertHash(ArrayList<Integer> values, String A) {
		assertThat(values, hasItem(HashedStringFieldType.hashCode(A)));
	}
	
	protected void assertFieldWithSet(HashedStringFieldData field,int docId,String[] set){
		
		assertThat(field.hasValue(docId),equalTo(set.length>0));
		
		final ArrayList<Integer> values = new ArrayList<Integer>();
		int missing = set.length >0 ? 0 : 1;
		assertThat(getDocHashes(docId, field, values),equalTo(missing));
		assertThat(values.size(),equalTo(set.length));
		for (String s : set ){
			assertHash(values,s);
		}

		assertThat(getDocOrdinals(docId, field, values),equalTo(missing));
		assertThat(values.size(),equalTo(set.length));

	}

	@Test
	public void singleValueHashedStringFieldDataTests() throws Exception {
		Directory dir = new RAMDirectory();
		IndexWriter indexWriter = new IndexWriter(dir, new IndexWriterConfig(
				Lucene.VERSION, Lucene.STANDARD_ANALYZER));

		indexWriter.addDocument(DocumentBuilder.doc()
				.add(DocumentBuilder.field("svalue", "zzz")).build());

		indexWriter.addDocument(DocumentBuilder.doc()
				.add(DocumentBuilder.field("svalue", "xxx")).build());

		indexWriter.addDocument(DocumentBuilder.doc().build());

		indexWriter.addDocument(DocumentBuilder.doc()
				.add(DocumentBuilder.field("svalue", "aaa")).build());

		indexWriter.addDocument(DocumentBuilder.doc()
				.add(DocumentBuilder.field("svalue", "aaa")).build());

		IndexReader reader = IndexReader.open(indexWriter, true);

		SingleValueHashedStringFieldData sFieldData = (SingleValueHashedStringFieldData) HashedStringFieldData
				.load(reader, "svalue");

		assert (sFieldData.fieldName().equals("svalue"));
		assert (!sFieldData.multiValued());
		assertThat(sFieldData.collisions(),equalTo(0));


		assertHash("zzz", sFieldData.hashValue(0));
		assertFieldWithSet(sFieldData, 0, new String[] { "zzz"});

		assertHash("xxx", sFieldData.hashValue(1));
		assertFieldWithSet(sFieldData, 1, new String[] { "xxx"});

		assertFieldWithSet(sFieldData, 2, new String[] {});

		assertHash("aaa", sFieldData.hashValue(3));
		assertFieldWithSet(sFieldData, 3, new String[] { "aaa"});

		assertHash("aaa", sFieldData.hashValue(4));
		assertFieldWithSet(sFieldData, 4, new String[] { "aaa"});
		

		indexWriter.close();
	}

	@Test
	public void singleValueHashedStringFieldData100Entires() throws Exception {
		Directory dir = new RAMDirectory();
		IndexWriter indexWriter = new IndexWriter(dir, new IndexWriterConfig(
				Lucene.VERSION, Lucene.STANDARD_ANALYZER));

		indexWriter.addDocument(DocumentBuilder.doc().build());

		for (int i = 0; i < 100; i++)
			indexWriter.addDocument(DocumentBuilder
					.doc()
					.add(DocumentBuilder.field("svalue",
							String.format("term_%s", i))).build());

		IndexReader reader = IndexReader.open(indexWriter, true);

		SingleValueHashedStringFieldData sFieldData = (SingleValueHashedStringFieldData) HashedStringFieldData
				.load(reader, "svalue");

		assertThat(sFieldData.fieldName(), equalTo("svalue"));
		assertThat(sFieldData.multiValued(), equalTo(false));
		assertThat(sFieldData.collisions(),equalTo(0));


		int[] sortedValues = Arrays.copyOf(sFieldData.values(),
				sFieldData.values().length);
		Arrays.sort(sortedValues,1,sortedValues.length);
		assertThat("Internal values of field data are not sorted!",
				sFieldData.values(), equalTo(sortedValues));

		assertThat(sFieldData.hasValue(0), equalTo(false));// first doc had no
															// value!

		for (int i = 0; i < 100; i++) {
			int docId = i + 1;
			assertThat(sFieldData.hasValue(docId), equalTo(true));
			String term = String.format("term_%s", i);
			assertHash(term, sFieldData.hashValue(docId));

			final ArrayList<Integer> values = new ArrayList<Integer>();
			getDocHashes(docId, sFieldData, values);
			assertThat(values.size(), equalTo(1));

			assertHash(values, term);

			values.clear();
		}
		


		indexWriter.close();
	}

	@Test
	public void multiValueHashedStringFieldDataTests() throws Exception {
		Directory dir = new RAMDirectory();
		IndexWriter indexWriter = new IndexWriter(dir, new IndexWriterConfig(
				Lucene.VERSION, Lucene.STANDARD_ANALYZER));

		indexWriter.addDocument(DocumentBuilder.doc()
				.add(DocumentBuilder.field("svalue", "zzz"))
				.add(DocumentBuilder.field("svalue", "xxx")).build());

		indexWriter.addDocument(DocumentBuilder.doc().build());

		indexWriter.addDocument(DocumentBuilder.doc()
				.add(DocumentBuilder.field("svalue", "aaa")).build());

		indexWriter.addDocument(DocumentBuilder.doc()
				.add(DocumentBuilder.field("svalue", "aaa")).build());

		IndexReader reader = IndexReader.open(indexWriter, true);

		MultiValueHashedStringFieldData sFieldData = (MultiValueHashedStringFieldData) HashedStringFieldData
				.load(reader, "svalue");

		assert (sFieldData.fieldName().equals("svalue"));
		assert (sFieldData.multiValued());
		assertThat(sFieldData.collisions(),equalTo(0));


		
		assertFieldWithSet(sFieldData, 0, new String[] { "zzz","xxx"});

		assertFieldWithSet(sFieldData, 1, new String[] { });

		assertFieldWithSet(sFieldData, 2, new String[] { "aaa" });
	
		assertFieldWithSet(sFieldData, 3, new String[] { "aaa" });

		indexWriter.close();
	}
	
	@Test
	public void TestMultiValueCollisionDetection() throws Exception {
		Directory dir = new RAMDirectory();
		IndexWriter indexWriter = new IndexWriter(dir, new IndexWriterConfig(
				Lucene.VERSION, new PatternAnalyzer(Version.LUCENE_36, PatternAnalyzer.NON_WORD_PATTERN, false, null)));

		indexWriter.addDocument(DocumentBuilder.doc()
				.add(DocumentBuilder.field("mvalue", "FB"))
				.add(DocumentBuilder.field("mvalue", "Ea")).build());

		indexWriter.addDocument(DocumentBuilder.doc()
				.add(DocumentBuilder.field("mvalue", "BB"))
				.add(DocumentBuilder.field("mvalue", "Aa")).build());

		
		IndexReader reader = IndexReader.open(indexWriter, true);

		MultiValueHashedStringFieldData sFieldData = (MultiValueHashedStringFieldData) HashedStringFieldData
				.load(reader, "mvalue");
		
		assertThat(sFieldData.collisions(),equalTo(2));
		indexWriter.close();

	}

   @Test
   public void TestMultiValueMaxTermsPerDoc() throws Exception {
      Directory dir = new RAMDirectory();
      IndexWriter indexWriter = new IndexWriter(dir, new IndexWriterConfig(
              Lucene.VERSION, new PatternAnalyzer(Version.LUCENE_36, PatternAnalyzer.WHITESPACE_PATTERN, false, null)));

      DocumentBuilder d = DocumentBuilder.doc();

      for (int i=0;i<200;i++) d.add(DocumentBuilder.field("mvalue", "t" + i));

      indexWriter.addDocument(d.build());

      indexWriter.addDocument(DocumentBuilder.doc()
              .add(DocumentBuilder.field("mvalue", "t1"))
              .add(DocumentBuilder.field("mvalue", "t2")).build());


      IndexReader reader = IndexReader.open(indexWriter, true);

      MultiValueHashedStringFieldData sFieldData = (MultiValueHashedStringFieldData) HashedStringFieldData
              .load(reader, "mvalue?max_terms_per_doc=199");

      assertThat(sFieldData.hasValue(0),equalTo(false));
      assertThat(sFieldData.hasValue(1),equalTo(true));
      indexWriter.close();

   }

   @Test
   public void TestMultiValueMinDocsPerTerm() throws Exception {
      Directory dir = new RAMDirectory();
      IndexWriter indexWriter = new IndexWriter(dir, new IndexWriterConfig(
              Lucene.VERSION, new PatternAnalyzer(Version.LUCENE_36, PatternAnalyzer.WHITESPACE_PATTERN, false, null)));

      DocumentBuilder d = DocumentBuilder.doc();

      d.add(DocumentBuilder.field("mvalue", "t1"));

      indexWriter.addDocument(d.build());

      indexWriter.addDocument(DocumentBuilder.doc()
              .add(DocumentBuilder.field("mvalue", "t1"))
              .add(DocumentBuilder.field("mvalue", "t2")).build());


      IndexReader reader = IndexReader.open(indexWriter, true);

      SingleValueHashedStringFieldData sFieldData = (SingleValueHashedStringFieldData) HashedStringFieldData
              .load(reader, "mvalue?min_docs_per_term=2");

      assertThat(sFieldData.hasValue(0),equalTo(true));
      final int[] count = {0};
      sFieldData.forEachValueInDoc(0, new HashedStringFieldData.HashedStringValueInDocProc() {
         @Override
         public void onValue(int docId, int Hash) {
            count[0]++;
         }

         @Override
         public void onMissing(int docId) {

         }
      });
      assertThat(count[0], equalTo(1));

      count[0]=0;
      sFieldData.forEachValueInDoc(1, new HashedStringFieldData.HashedStringValueInDocProc() {
         @Override
         public void onValue(int docId, int Hash) {
            count[0]++;
         }

         @Override
         public void onMissing(int docId) {

         }
      });
      assertThat(sFieldData.hasValue(1), equalTo(true));
      assertThat(count[0], equalTo(1));
      indexWriter.close();

   }



   protected int getDocHashes(int docId, HashedStringFieldData sFieldData,
			final ArrayList<Integer> values) {
		values.clear();
		final ArrayList<Integer> missing = new ArrayList<Integer>();
		sFieldData.forEachValueInDoc(docId,
				new HashedStringFieldData.HashedStringValueInDocProc() {
					public void onValue(int docId, int Hash) {
						values.add(Hash);
					}

					public void onMissing(int docId) {
						missing.add(docId);
					}
				});
		return missing.size();
	}
	
	protected int getDocOrdinals(final int docId, HashedStringFieldData sFieldData,
			final ArrayList<Integer> values) {
		values.clear();
		final ArrayList<Integer> missing = new ArrayList<Integer>();
		sFieldData.forEachOrdinalInDoc(docId,
				new OrdinalInDocProc() {
				
					public void onOrdinal(int int_docId, int ordinal) {
						assertThat(int_docId,equalTo(docId));
						if (ordinal == 0)
							missing.add(int_docId);
						else
							values.add(ordinal);
						
					}
				});
		return missing.size();
	}

   @Test
   public void ResolvingHashTest(){
      NamedAnalyzer analyzer = Lucene.STANDARD_ANALYZER;
      String value = "Some text with spaces";
      String term = HashedStringFieldData.analyzeStringForTerm(value,
              HashedStringFieldType.hashCode("spaces"), "field", analyzer);
      assertThat(term,equalTo("spaces"));
      value = "Some other text with spaces and more";
      term = HashedStringFieldData.analyzeStringForTerm(value,
              HashedStringFieldType.hashCode("more"), "field", analyzer);
      assertThat(term,equalTo("more"));
   }

}