package org.leskes.elasticfacets.fields;

import org.elasticsearch.common.RamUsage;

public class MultiValueHashedStringFieldData extends HashedStringFieldData {
	
	// order with value -1 indicates no value
    private final int[][] ordinals;

	
    public MultiValueHashedStringFieldData(String field, int[] sorted_values,
			int[][] ordinals) {
		super(field,sorted_values);
		this.ordinals = ordinals;
	}
    
    @Override
    protected long computeSizeInBytes() {
        long size = super.computeSizeInBytes();
        size += RamUsage.NUM_BYTES_ARRAY_HEADER; // for the top level array
        for (int[] ordinal : ordinals) {
            size += RamUsage.NUM_BYTES_INT * ordinal.length + RamUsage.NUM_BYTES_ARRAY_HEADER;
        }
        return size;
    }

    @Override
	public boolean multiValued() {
		return true;
	}

    @Override
    public boolean hasValue(int docId) {
        for (int[] ordinal : ordinals) {
            if (ordinal[docId] >= 0) {
                return true;
            }
        }
        return false;
    }

	public void forEachValueInDoc(int docId, HashedStringValueInDocProc proc) {
		boolean hadValue = false;
		for (int[] ordinal : ordinals) {
			int loc = ordinal[docId];
			if (loc < 0) {
				break;
			}
			hadValue = true;
			proc.onValue(docId, values[loc]);
		}
		if (!hadValue)
			proc.onMissing(docId);
	}
    
    @Override
    public void forEachOrdinalInDoc(int docId, OrdinalInDocProc proc) {
    	int i=0;
    	int loc = ordinals[i][docId];
    	proc.onOrdinal(docId, loc);
		for (i++;i<ordinals.length;i++) {
			loc = ordinals[i][docId];
			if (loc < 0) {
				// need to report missing here - we already sent a value.
				break;
			}
			proc.onOrdinal(docId, loc);
		}
    }

	
}
