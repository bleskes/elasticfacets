package org.leskes.elasticfacets.fields;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;

import org.apache.lucene.index.IndexReader;
import org.elasticsearch.common.RamUsage;
import org.elasticsearch.common.trove.list.array.TIntArrayList;
import org.elasticsearch.index.field.data.FieldData;
import org.elasticsearch.index.field.data.FieldDataType;
import org.elasticsearch.index.field.data.support.FieldDataLoader;

/**
 *
 */
public abstract class HashedStringFieldData extends FieldData<HashedStringDocFieldData> {

	public static final HashedStringFieldType HASHED_STRING = new HashedStringFieldType();
	
    protected final int[] values;

    protected HashedStringFieldData(String fieldName, int[] values) {
        super(fieldName);
        this.values = values;
    }
    
    public int[] values() {
    	return values;
    }

    @Override
    protected long computeSizeInBytes() {
        long size = RamUsage.NUM_BYTES_ARRAY_HEADER;
        size += values.length * RamUsage.NUM_BYTES_INT;
        return size;
    }

    
    @Override
    public HashedStringDocFieldData docFieldData(int docId) {
        return super.docFieldData(docId);
    }

	
	@Override
	public void forEachValue(StringValueProc proc) {
        throw new UnsupportedOperationException("HashedStringData doesn't support string iteration. Those are gone");
	}

	@Override
	public void forEachValueInDoc(
			int docId,StringValueInDocProc proc) {
        throw new UnsupportedOperationException("HashedStringData doesn't support string iteration. Those are gone");
		
	}
	
    abstract public void forEachValueInDoc(int docId, HashedStringValueInDocProc proc);


    @Override
    public String stringValue(int docId) {
        throw new UnsupportedOperationException("Hashed string field data destroyes original valus");
    }

    @Override
    protected HashedStringDocFieldData createFieldData() {
        return new HashedStringDocFieldData(this);
    }

    @Override
    public FieldDataType type() {
        return HashedStringFieldData.HASHED_STRING;
    }
    
    public static interface HashedStringValueInDocProc {
        void onValue(int docId,int Hash);
        void onMissing(int docId);
    }

   
    public static HashedStringFieldData load(IndexReader reader, String field) throws IOException {
        return FieldDataLoader.load(reader, field, new HashedStringTypeLoader());
    }

    static class HashedStringTypeLoader extends FieldDataLoader.FreqsTypeLoader<HashedStringFieldData> {

    	 private final TIntArrayList hashed_terms = new TIntArrayList();
    	 
    	 private int[] sorted_hashed_terms;
    	 private int[] new_location_of_hashed_terms_in_sorted;

    	 HashedStringTypeLoader() {
             super();
             // the first one indicates null value.
             hashed_terms.add(0);

         }

        public void collectTerm(String term) {
        	hashed_terms.add(HashedStringFieldType.hashCode(term));
        }
        
        protected void sort_values() {
        	// as we hashed the values they are not sorted. They need to be for proper working of the rest. 
        	Integer[] translation_indices = new Integer[hashed_terms.size()-1]; // drop the first "non value place"
        	for (int i=0;i<translation_indices.length;i++) translation_indices[i]=i+1; // one ofsset for the dropped place
        	Arrays.sort(translation_indices, new Comparator<Integer>() {

				public int compare(Integer paramT1, Integer paramT2) {
					int d1 = hashed_terms.get(paramT1);
					int d2 = hashed_terms.get(paramT2);
					return d1 < d2 ? -1 : (d1 == d2 ? 0 : 1);
				}}
        	);
        	
        	
        	// now build a sorted array and update the ordinal values (added the n value in the beginning)
        	sorted_hashed_terms =  new int[hashed_terms.size()-1];
        	new_location_of_hashed_terms_in_sorted = new int[hashed_terms.size()];
        	
        	for (int i=0;i<translation_indices.length;i++) { 
        		sorted_hashed_terms[i]=hashed_terms.get(translation_indices[i]);
        		new_location_of_hashed_terms_in_sorted[translation_indices[i]]=i;
        	}
        	
        	// before the sorting the first value (0) was indicating docs with no values. After sorting 0 has moved
        	// We could use the value as indication but that's an assumption of it being an illegal hash value.
        	// Rather then that - we assign an ordinal of -1.
        	new_location_of_hashed_terms_in_sorted[0]=-1;

	
        }
        
		protected void updateOrdinalArray(int[] ordinals) {
			for (int i=0;i<ordinals.length;i++) 
        		ordinals[i]=new_location_of_hashed_terms_in_sorted[ordinals[i]];
		}


        public HashedStringFieldData buildSingleValue(String field, int[] ordinals) {
        	
        	sort_values();
        	
        	updateOrdinalArray(ordinals);
        	
            return new SingleValueHashedStringFieldData(field,sorted_hashed_terms,ordinals);
        }


        public HashedStringFieldData buildMultiValue(String field, int[][] ordinals) {
        	sort_values();
        	
        	for (int[] ordinal: ordinals) {
        		updateOrdinalArray(ordinal);	
        	}
        	
        	
            return new MultiValueHashedStringFieldData(field,sorted_hashed_terms,ordinals);
        }
    }


}
