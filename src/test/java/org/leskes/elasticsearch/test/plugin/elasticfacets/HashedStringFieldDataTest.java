package org.leskes.elasticsearch.test.plugin.elasticfacets;

import java.util.ArrayList;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.elasticsearch.common.lucene.DocumentBuilder;
import org.elasticsearch.common.lucene.Lucene;
import org.leskes.elasticsearch.plugin.elasticfacets.HashedStringFieldData;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

public class HashedStringFieldDataTest
{
  protected void assertHash(String A, String B)
  {
    AssertJUnit.assertEquals("Hash code of " + A + " doesn't equal the one of " + B, A.hashCode(), B.hashCode());
  }

  protected void assertHash(int A, String B) {
    AssertJUnit.assertEquals("Hash code doesn't equal the one of " + B, A, B.hashCode());
  }

  protected void assertHash(String A, int B)
  {
    AssertJUnit.assertEquals("Hash code doesn't equal the one of " + A, A.hashCode(), B);
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
}