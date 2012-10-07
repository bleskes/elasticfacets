/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

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
public class HashedStringFieldData extends FieldData<HashedStringDocFieldData> {

	public static final HashedStringFieldType HASHED_STRING = new HashedStringFieldType();
	
    protected final int[] values;
    // order with value -1 indicates no value
    protected final int[] ordinals;

    protected HashedStringFieldData(String fieldName, int[] values,int[] ordinals) {
        super(fieldName);
        this.values = values;
        this.ordinals = ordinals;
    }
    
    public int[] values() {
    	return values;
    }

    @Override
    protected long computeSizeInBytes() {
        long size = 2*RamUsage.NUM_BYTES_ARRAY_HEADER;
        size += 2*values.length * RamUsage.NUM_BYTES_INT;
        return size;
    }


    @Override
    public HashedStringDocFieldData docFieldData(int docId) {
        return super.docFieldData(docId);
    }

	@Override
	public boolean multiValued() {
		return false;
	}

	@Override
	public boolean hasValue(int docId) {
		return ordinals[docId] >=0;
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
	

    @Override
    public String stringValue(int docId) {
        throw new UnsupportedOperationException("Hashed string field data destroyes original valus");
    }
    
    public int hashValue(int docId) {
    	return values[ordinals[docId]];
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


    public void forEachValueInDoc(int docId, HashedStringValueInDocProc proc) {
        int loc = ordinals[docId];
        if (loc < 0) {
            proc.onMissing(docId);
            return;
        }
        proc.onValue(docId, values[loc]);
    }
    
    @Override
    public void forEachOrdinalInDoc(int docId, OrdinalInDocProc proc) {
        proc.onOrdinal(docId, ordinals[docId]);
    }


   
    public static HashedStringFieldData load(IndexReader reader, String field) throws IOException {
        return FieldDataLoader.load(reader, field, new HashedStringTypeLoader());
    }

    static class HashedStringTypeLoader extends FieldDataLoader.FreqsTypeLoader<HashedStringFieldData> {

    	 private final TIntArrayList hashed_terms = new TIntArrayList();

    	 HashedStringTypeLoader() {
             super();
             // the first one indicates null value.
             hashed_terms.add(0);

         }

        public void collectTerm(String term) {
        	hashed_terms.add(HashedStringFieldType.hashCode(term));
        }

        public HashedStringFieldData buildSingleValue(String field, int[] ordinals) {
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
        	int[] sorted_values =  new int[hashed_terms.size()-1];
        	
        	
        	int[] new_location_of_old_index = new int[hashed_terms.size()];
        	for (int i=0;i<translation_indices.length;i++) { 
        		sorted_values[i]=hashed_terms.get(translation_indices[i]);
        		new_location_of_old_index[translation_indices[i]]=i;
        	}
        	
        	// before the soring the first value (0) was indicating docs with no values. After sorting 0 has moved
        	// We could use the value as indication but that's an assumption of it being an illegal hash value.
        	// Rather then that - we assign an ordinal of -1.
        	new_location_of_old_index[0]=-1;
        	
        	for (int i=0;i<ordinals.length;i++) 
        		ordinals[i]=new_location_of_old_index[ordinals[i]];
        	
            return new HashedStringFieldData(field,sorted_values,ordinals);
        }

        public HashedStringFieldData buildMultiValue(String field, int[][] ordinals) {
            throw new UnsupportedOperationException("Multi values fields are not implemented by HashedStringTypes");
        }
    }


}
