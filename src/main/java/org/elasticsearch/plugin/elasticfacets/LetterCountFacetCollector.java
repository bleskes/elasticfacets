package org.elasticsearch.plugin.elasticfacets;

import java.io.IOException;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Scorer;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.index.cache.field.data.FieldDataCache;
import org.elasticsearch.index.field.data.FieldData.StringValueInDocProc;
import org.elasticsearch.index.field.data.FieldDataType;
import org.elasticsearch.index.field.data.NumericFieldData;
import org.elasticsearch.index.field.data.strings.StringFieldData;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.search.facet.AbstractFacetCollector;
import org.elasticsearch.search.facet.Facet;
import org.elasticsearch.search.facet.FacetCollector;
import org.elasticsearch.search.facet.FacetPhaseExecutionException;
import org.elasticsearch.search.internal.SearchContext;

public class LetterCountFacetCollector extends AbstractFacetCollector implements StringValueInDocProc {

	final static ESLogger logger = Loggers.getLogger(LetterCountFacetCollector.class);

	
	private final char C;
	private final String indexFieldName;
	private final FieldDataType fieldDataType;
	private final FieldDataCache fieldDataCache;
	private StringFieldData fieldData;
	
	private long count = 0L;
	
	
	public LetterCountFacetCollector(String facetName, String field, char C,
			SearchContext context) {
		super(facetName);
		this.C = C;
		
		this.fieldDataCache = context.fieldDataCache();
		MapperService.SmartNameFieldMappers smartMappers = context.smartFieldMappers(field);
        if (smartMappers == null || !smartMappers.hasMapper()) {
            throw new FacetPhaseExecutionException(facetName, "No mapping found for field [" + field + "]");
        }

        // add type filter if there is exact doc mapper associated with it
        if (smartMappers.explicitTypeInNameWithDocMapper()) {
            setFilter(context.filterCache().cache(smartMappers.docMapper().typeFilter()));
        }

        indexFieldName = smartMappers.mapper().names().indexName();
        fieldDataType = smartMappers.mapper().fieldDataType();
	}

	@Override
	protected void doSetNextReader(IndexReader reader, int docBase)
			throws IOException {
		fieldData = (StringFieldData) fieldDataCache.cache(fieldDataType, reader, indexFieldName);
		
	}

	@Override
	protected void doCollect(int doc) throws IOException {
		logger.debug("collecting {}", doc);
		fieldData.forEachValueInDoc(doc, this);
		
	}
	
	public void onValue(int docId, String value) {
		for (int i=0;i<value.length();i++)
			if (value.charAt(i) == this.C)
				count++;
		
	}

	public void onMissing(int docId) {
		// we only care about occurrences of character C, so missing values are of no interest. 
		
	}


	@Override
	public Facet facet() {
		return new LetterCountFacet(facetName,count);
	}

	
	

}
