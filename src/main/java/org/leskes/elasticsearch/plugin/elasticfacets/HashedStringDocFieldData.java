package org.leskes.elasticsearch.plugin.elasticfacets;

import org.elasticsearch.index.field.data.DocFieldData;
import org.elasticsearch.index.field.data.FieldData;

public class HashedStringDocFieldData extends DocFieldData<HashedStringFieldData> {

	protected HashedStringDocFieldData(HashedStringFieldData fieldData) {
		super(fieldData);
	}

}
