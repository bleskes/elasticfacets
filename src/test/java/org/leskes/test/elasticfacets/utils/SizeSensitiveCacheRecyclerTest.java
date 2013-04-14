package org.leskes.test.elasticfacets.utils;

import junit.framework.TestCase;
import org.leskes.elasticfacets.utils.SizeSensitiveCacheRecycler;
import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class SizeSensitiveCacheRecyclerTest extends TestCase {
   @Test
   public void testPopIntArray() throws Exception {
      SizeSensitiveCacheRecycler.clear();
      int[] array = SizeSensitiveCacheRecycler.popIntArray(10);
      assertThat(array.length, equalTo(10));

      SizeSensitiveCacheRecycler.pushIntArray(array);

      array = SizeSensitiveCacheRecycler.popIntArray(4);
      assertThat(array.length, equalTo(10));

      array = SizeSensitiveCacheRecycler.popIntArray(4);
      assertThat(array.length, equalTo(4));
      SizeSensitiveCacheRecycler.pushIntArray(array);

      array = SizeSensitiveCacheRecycler.popIntArray(1000);
      assertThat(array.length, equalTo(1000));
      SizeSensitiveCacheRecycler.pushIntArray(array);

      array = SizeSensitiveCacheRecycler.popIntArray(600);
      assertThat(array.length, equalTo(1000));

   }


}
