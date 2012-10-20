package org.leskes.elasticfacets.fields;

import org.elasticsearch.common.RamUsage;

public class SingleValueHashedStringFieldData extends HashedStringFieldData {
	
	// order with value -1 indicates no value
    protected final int[] ordinals;

	
    public SingleValueHashedStringFieldData(String field, int[] sorted_values,
			int[] ordinals) {
		super(field,sorted_values);
		this.ordinals = ordinals;
	}
    
    @Override
    protected long computeSizeInBytes() {
        long size = super.computeSizeInBytes(); 
        size += RamUsage.NUM_BYTES_ARRAY_HEADER;
        size += ordinals.length * RamUsage.NUM_BYTES_INT;
        return size;
    }

    @Override
	public boolean multiValued() {
		return false;
	}

	@Override
	public boolean hasValue(int docId) {
		return ordinals[docId] >=0;
	}

    public int hashValue(int docId) {
    	return values[ordinals[docId]];
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

	
	
}
