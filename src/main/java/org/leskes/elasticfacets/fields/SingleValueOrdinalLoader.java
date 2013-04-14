package org.leskes.elasticfacets.fields;

public class SingleValueOrdinalLoader implements OrdinalLoader {

   private final int[] ordinals;

   public SingleValueOrdinalLoader(int docCount) {
      this.ordinals = new int[docCount];
   }

   @Override
   public void addDocOrdinal(int docId, int ordinal) {
      ordinals[docId] = ordinal;
   }

   public int[]  getOrdinals() {
      return ordinals;
   }
}
