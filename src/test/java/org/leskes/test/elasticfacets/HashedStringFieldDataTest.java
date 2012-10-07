package org.leskes.test.elasticfacets;

import java.util.ArrayList;
import java.util.Arrays;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.elasticsearch.common.lucene.DocumentBuilder;
import org.elasticsearch.common.lucene.Lucene;
import org.leskes.elasticfacets.fields.HashedStringFieldData;
import org.leskes.elasticfacets.fields.HashedStringFieldType;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;


public class HashedStringFieldDataTest
{
  protected void assertHash(String A, String B)
  {
    AssertJUnit.assertEquals("Hash code of " + A + " doesn't equal the one of " + B, 
    		HashedStringFieldType.hashCode(A),HashedStringFieldType.hashCode(B));
  }

  protected void assertHash(int A, String B) {
    AssertJUnit.assertEquals("Hash code doesn't equal the one of " + B, A, HashedStringFieldType.hashCode(B));
  }

  protected void assertHash(String A, int B)
  {
    AssertJUnit.assertEquals("Hash code doesn't equal the one of " + A, HashedStringFieldType.hashCode(A), B);
  }

  @Test
  public void hashedStringFieldDataTests() throws Exception
  {
    Directory dir = new RAMDirectory();
    IndexWriter indexWriter = new IndexWriter(dir, new IndexWriterConfig(Lucene.VERSION, Lucene.STANDARD_ANALYZER));

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

    HashedStringFieldData sFieldData = HashedStringFieldData.load(reader, "svalue");

    assert (sFieldData.fieldName().equals("svalue"));
    assert (!sFieldData.multiValued());

    AssertJUnit.assertTrue(sFieldData.hasValue(0));
    assertHash("zzz", sFieldData.hashValue(0));

    AssertJUnit.assertTrue(sFieldData.hasValue(1));
    assertHash("xxx", sFieldData.hashValue(1));

    AssertJUnit.assertFalse(sFieldData.hasValue(2));

    AssertJUnit.assertTrue(sFieldData.hasValue(3));
    assertHash("aaa", sFieldData.hashValue(3));

    AssertJUnit.assertTrue(sFieldData.hasValue(4));
    assertHash("aaa", sFieldData.hashValue(4));

    final ArrayList values = new ArrayList();
    sFieldData.forEachValueInDoc(1, new HashedStringFieldData.HashedStringValueInDocProc()
    {
      public void onValue(int docId, int Hash) {
        values.add(Integer.valueOf(Hash));
      }

      public void onMissing(int docId)
      {
      }
    });
    assert (values.size() == 1);

    assertHash(((Integer)values.get(0)).intValue(), "xxx");

    values.clear();

    indexWriter.close();
  }

  @Test
  public void hashedStringFieldData100Entires() throws Exception
  {
    Directory dir = new RAMDirectory();
    IndexWriter indexWriter = new IndexWriter(dir, new IndexWriterConfig(Lucene.VERSION, Lucene.STANDARD_ANALYZER));

    indexWriter.addDocument(DocumentBuilder.doc().build());

    
    for (int i=0;i<100;i++)
	    indexWriter.addDocument(DocumentBuilder.doc()
	      .add(DocumentBuilder.field("svalue", String.format("term_%s",i))).build());

    IndexReader reader = IndexReader.open(indexWriter, true);

    HashedStringFieldData sFieldData = HashedStringFieldData.load(reader, "svalue");

    assertThat(sFieldData.fieldName(),equalTo("svalue"));
    assertThat(sFieldData.multiValued(),equalTo(false));
    
    int[] sortedValues = Arrays.copyOf(sFieldData.values(), sFieldData.values().length);
    Arrays.sort(sortedValues);
    assertThat("Internal values of field data are not sorted!", sFieldData.values(), equalTo(sortedValues));

	assertThat(sFieldData.hasValue(0),equalTo(false));// first doc had no value!
    
    
    for (int i=0;i<100;i++) {
    	int docId= i+1;
    	assertThat(sFieldData.hasValue(docId),equalTo(true));
    	String term = String.format("term_%s",i);
    	assertHash(term,sFieldData.hashValue(docId));

    	final ArrayList<Integer> values = new ArrayList<Integer>();

        sFieldData.forEachValueInDoc(docId, new HashedStringFieldData.HashedStringValueInDocProc()
        {
          public void onValue(int docId, int Hash) {
            values.add(Integer.valueOf(Hash));
          }

          public void onMissing(int docId)
          {
          }
        });
        assertThat(values.size(),equalTo(1));

        assertHash(((Integer)values.get(0)).intValue(), term);

        values.clear();
    }

    

    indexWriter.close();
  }


}