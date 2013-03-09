package org.leskes.elasticfacets.fields;

public class MultiValueHashedStringFieldData extends HashedStringFieldData {
	
	 private final MultiValueOrdinalArray ordinals;

	
    public MultiValueHashedStringFieldData(String field, int[] sorted_values,
			MultiValueOrdinalArray ordinals) {
		super(field,sorted_values);
		this.ordinals = ordinals;
	}
    
    @Override
    protected long computeSizeInBytes() {
       long size = super.computeSizeInBytes();
       size += ordinals.computeSizeInBytes();
       return size;
    }

    @Override
	public boolean multiValued() {
		return true;
	}

    @Override
    public boolean hasValue(int docId) {
       return ordinals.hasValue(docId);
    }

	public void forEachValueInDoc(int docId, HashedStringValueInDocProc proc) {
      MultiValueOrdinalArray.OrdinalIterator ordinalIter = ordinals.getOrdinalIteratorForDoc(docId);
      int o = ordinalIter.getNextOrdinal();
      if (o == 0) {
         proc.onMissing(docId); // first one is special as we need to communicate 0 if nothing is found
         return;
      }

      while (o != 0) {
         proc.onValue(docId, values[o]);
         o = ordinalIter.getNextOrdinal();
      }
	}
    
    @Override
    public void forEachOrdinalInDoc(int docId, OrdinalInDocProc proc) {
       ordinals.forEachOrdinalInDoc(docId, proc);
    }

	
}
