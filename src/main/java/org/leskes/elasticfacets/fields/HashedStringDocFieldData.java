package org.leskes.elasticfacets.fields;

import org.elasticsearch.index.field.data.DocFieldData;
import org.elasticsearch.index.field.data.FieldData;

public class HashedStringDocFieldData extends DocFieldData<HashedStringFieldData> {

	protected HashedStringDocFieldData(HashedStringFieldData fieldData) {
		super(fieldData);
	}

}
