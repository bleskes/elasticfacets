package org.leskes.elasticfacets.utils;

import org.elasticsearch.common.util.concurrent.ConcurrentCollections;

import java.lang.ref.SoftReference;
import java.util.Arrays;
import java.util.Queue;


public class SizeSensitiveCacheRecycler {

   public static void clear() {
      for (SoftWrapper ia: intArrays)
         ia.clear();
   }

   private static final int[] sizeMasks;

   private static SoftWrapper[] intArrays;

   static  {
      sizeMasks = new int[] {
              128,
              512,
              1024,
              2048,
              16384,
              65536,
              262144,
              1048576,
              8388608
      };
      intArrays = new SoftWrapper[sizeMasks.length + 1];

      for (int i=0;i<=sizeMasks.length;i++) {
         intArrays[i] = new SoftWrapper();
      }
   }

   private static SoftWrapper getArrayQueueForSize(int size) {
     for (int i=0;i<sizeMasks.length;i++)
        if (size < sizeMasks[i]) {
           return intArrays[i];
        }
      return intArrays[sizeMasks.length];
   }

   public static int[] popIntArray(int size) {
      return popIntArray(size, 0);
   }

   public static int[] popIntArray(int size, int sentinal) {
      SoftWrapper intArray = getArrayQueueForSize(size);
      Queue<int[]> ref = intArray.get();
      if (ref == null) {
         int[] ints = new int[size];
         if (sentinal != 0) {
            Arrays.fill(ints, sentinal);
         }
         return ints;
      }
      int[] ints = ref.poll();
      if (ints == null) {
         ints = new int[size];
         if (sentinal != 0) {
            Arrays.fill(ints, sentinal);
         }
         return ints;
      }
      if (ints.length < size) {
         ints = new int[size];
         if (sentinal != 0) {
            Arrays.fill(ints, sentinal);
         }
         return ints;
      }
      return ints;
   }

   public static void pushIntArray(int[] ints) {
      pushIntArray(ints, 0);
   }

   public static void pushIntArray(int[] ints, int sentinal) {
      SoftWrapper intArray = getArrayQueueForSize(ints.length);
      Queue<int[]> ref = intArray.get();
      if (ref == null) {
         ref = ConcurrentCollections.newQueue();
         intArray.set(ref);
      }
      Arrays.fill(ints, sentinal);
      ref.add(ints);
   }


   static class SoftWrapper {
      private SoftReference<Queue<int[]>> ref;

      public SoftWrapper() {
      }

      public void set(Queue<int[]> ref) {
         this.ref = new SoftReference<Queue<int[]>>(ref);
      }

      public Queue<int[]> get() {
         return ref == null ? null : ref.get();
      }

      public void clear() {
         ref = null;
      }
   }
}
