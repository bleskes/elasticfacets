package org.leskes.test.elasticfacets.fields;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.common.RamUsage;
import org.elasticsearch.common.trove.list.array.TIntArrayList;
import org.elasticsearch.index.field.data.FieldData;
import org.leskes.elasticfacets.fields.MultiValueOrdinalArray;
import org.testng.annotations.Test;

import java.util.ArrayList;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class MultiValueOrdinalArrayTests {

   protected int STORAGE_SIZE = 8;

   class smallMultiValueOrdinalArray extends MultiValueOrdinalArray {

      public smallMultiValueOrdinalArray(int[] ordinalsNoPerDoc) {
         super(ordinalsNoPerDoc,STORAGE_SIZE);
      }
   }

   protected TIntArrayList collectOrdinals(MultiValueOrdinalArray a,int docId) {
      final TIntArrayList ol = new TIntArrayList();
      a.forEachOrdinalInDoc(docId, new FieldData.OrdinalInDocProc() {
         @Override
         public void onOrdinal(int docId, int ordinal) {
            ol.add(ordinal);
         }
      });
      return ol;
   }

   protected void regressToArray(ArrayList<int[]> ordinalsPerDoc) {
      // invert ordinalsPerDoc to the structure MultiValueOrdinalArray expects

      smallMultiValueOrdinalArray a = getSmallMultiValueOrdinalArray(ordinalsPerDoc);

      for (int doc=0;doc<ordinalsPerDoc.size();doc++) {
         assertThat(collectOrdinals(a, doc),
                 equalTo(new TIntArrayList(ordinalsPerDoc.get(doc))));
      }
   }

   private smallMultiValueOrdinalArray getSmallMultiValueOrdinalArray(ArrayList<int[]> ordinalsPerDoc) {
      int[] ordinalsNoPerDoc = new int[ordinalsPerDoc.size()];
      for (int doc=0; doc < ordinalsNoPerDoc.length; doc++) {
         ordinalsNoPerDoc[doc] = ordinalsPerDoc.get(doc).length;
      }

      smallMultiValueOrdinalArray ret = new smallMultiValueOrdinalArray(ordinalsNoPerDoc);

      MultiValueOrdinalArray.MultiValueOrdinalLoader loader = ret.createLoader();

      for (int doc=0;doc<ordinalsPerDoc.size();doc++) {
         for (int o : ordinalsPerDoc.get(doc))
            loader.addDocOrdinal(doc,o);
      }

      return ret;
   }

   @Test
   public void testSingleValue() {
      ArrayList<int[]> o = new ArrayList<int[]>();
      o.add(new int[] {1});
      o.add(new int[] {0});
      o.add(new int[] {1});
      o.add(new int[] {2});
      o.add(new int[] {3});

      regressToArray(o);
   }

   @Test
   public void testSingleDocMultiValue() {
      ArrayList<int[]> o = new ArrayList<int[]>();
      o.add(new int[] {1,2});
      regressToArray(o);
   }


   @Test
   public void testStorageOverflow() {
      ArrayList<int[]> o = new ArrayList<int[]>();
      o.add(new int[] {1});
      o.add(new int[] {0});
      o.add(new int[] {1});
      o.add(new int[] {1,2,3,4,5,6});
      o.add(new int[] {3});
      o.add(new int[] {1,2,3,4});
      regressToArray(o);
   }


   @Test
   public void testComputeSizeInBytes() {
      ArrayList<int[]> o = new ArrayList<int[]>();
      o.add(new int[] {1});
      o.add(new int[] {0});
      o.add(new int[] {1});
      o.add(new int[] {1,2,3,4,5,6});
      smallMultiValueOrdinalArray a = getSmallMultiValueOrdinalArray(o);
      assertThat(a.computeSizeInBytes(),greaterThan(9L* RamUsage.NUM_BYTES_INT));
   }

   @Test
   public void testHasValue() {
      ArrayList<int[]> o = new ArrayList<int[]>();
      o.add(new int[] {1});
      o.add(new int[] {0});
      o.add(new int[] {1});
      o.add(new int[] {1,2,3,4,5,6});
      smallMultiValueOrdinalArray a = getSmallMultiValueOrdinalArray(o);
      assertThat(a.hasValue(0),equalTo(true));
      assertThat(a.hasValue(1),equalTo(false));
      assertThat(a.hasValue(2),equalTo(true));
      assertThat(a.hasValue(3),equalTo(true));
   }

   @Test(expectedExceptions = { ElasticSearchException.class } )
   public void testImpossibleStorageOverflow() {
      ArrayList<int[]> o = new ArrayList<int[]>();
      o.add(new int[] {1});
      o.add(new int[] {0});
      o.add(new int[] {1});
      o.add(new int[] {1,2,3,4,5,6,7,8});
      o.add(new int[] {3});
      regressToArray(o);
   }

}
