package org.leskes.elasticfacets;

import org.apache.lucene.index.IndexReader;
import org.elasticsearch.common.CacheRecycler;
import org.elasticsearch.common.joda.TimeZoneRounding;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.trove.ExtTLongObjectHashMap;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.cache.field.data.FieldDataCache;
import org.elasticsearch.index.field.data.FieldDataType;
import org.elasticsearch.index.field.data.longs.LongFieldData;
import org.elasticsearch.index.field.data.strings.StringFieldData;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.search.facet.*;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;




/**
 * A histogram facet collector that uses different fields for the key and the
 * value.
 */
public class FacetedDateHistogramCollector extends
		AbstractFacetCollector {

	private final String keyIndexFieldName;
	private final String keyFieldName;


	private final FieldDataCache fieldDataCache;

	private final FieldDataType keyFieldDataType;
	private LongFieldData keyFieldData;

	private final DateHistogramProc histoProc;
	
	private final FacetCollector internalExampleCollector;
	
	private static ESLogger logger = Loggers.getLogger(FacetedDateHistogramCollector.class);



	public FacetedDateHistogramCollector(String facetName,
			String keyFieldName, 
			TimeZoneRounding tzRounding,
			FacetProcessor internalProcessor,
			byte[] internalFacetConfig,
			SearchContext context) throws IOException {
		super(facetName);
		this.fieldDataCache = context.fieldDataCache();
		this.keyFieldName = keyFieldName;
		MapperService.SmartNameFieldMappers smartMappers = context
				.smartFieldMappers(keyFieldName);
		if (smartMappers == null || !smartMappers.hasMapper()) {
			throw new FacetPhaseExecutionException(facetName,
					"No mapping found for field [" + keyFieldName + "]");
		}

		// add type filter if there is exact doc mapper associated with it
		if (smartMappers.explicitTypeInNameWithDocMapper()) {
			setFilter(context.filterCache().cache(
					smartMappers.docMapper().typeFilter()));
		}

		keyIndexFieldName = smartMappers.mapper().names().indexName();
		keyFieldDataType = smartMappers.mapper().fieldDataType();
		
		InternalCollectorFactory  colFactory= new InternalCollectorFactory(facetName, internalProcessor, internalFacetConfig, context);
		
		logger.debug("Facet {}: Test running internal facet processor ", facetName);
		this.internalExampleCollector = colFactory.createInternalCollector();

		this.histoProc = new DateHistogramProc(facetName,tzRounding,colFactory);
	}
	
	protected static class InternalCollectorFactory {
		private FacetProcessor internalProcessor;
		private byte[] internalFacetConfig;
		private SearchContext searchContext;
		private String facetName;
		
		public InternalCollectorFactory(String facetName,FacetProcessor internalProcessor,byte[] internalFacetConfig, SearchContext searchContext)
				{
			this.internalProcessor = internalProcessor;
			this.internalFacetConfig = internalFacetConfig;
			this.searchContext = searchContext;
			this.facetName = facetName;
		}
		
		
		public FacetCollector createInternalCollector() throws IOException {
			XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(internalFacetConfig);
	        try {
	            return internalProcessor.parse("facet", parser, searchContext);
	        } finally {
	            parser.close();
	        }
		}

	}
	
	@Override
	public boolean acceptsDocsOutOfOrder() {
		return internalExampleCollector.acceptsDocsOutOfOrder();
	}


	@Override
	protected void doCollect(int doc) throws IOException {
		keyFieldData.forEachValueInDoc(doc, histoProc);
	}

	@Override
	protected void doSetNextReader(IndexReader reader, int docBase)
			throws IOException {
		keyFieldData = (LongFieldData) fieldDataCache.cache(keyFieldDataType,
				reader, keyIndexFieldName);
		
		histoProc.setNextReader(reader, docBase);
	}

	@Override
	public Facet facet() {
		for (Object o: histoProc.entries.internalValues()){
			if (o == null) continue;
			((FacetedDateHistogramFacet.Entry)o).facetize();
		}
		return new FacetedDateHistogramFacet(facetName,histoProc.entries);
	}
	
	
	
	public static class DateHistogramProc implements
			LongFieldData.LongValueInDocProc {

		final ExtTLongObjectHashMap<FacetedDateHistogramFacet.Entry> entries = CacheRecycler
				.popLongObjectMap();
		protected final TimeZoneRounding tzRounding;

		StringFieldData stringFieldData = null;

		final protected  InternalCollectorFactory collectorFactory;
		
		protected IndexReader currentIndexer;
		protected int currentDocBase;

		public DateHistogramProc(String facetName, TimeZoneRounding tzRounding, InternalCollectorFactory collectorFactor)
		{
			this.tzRounding = tzRounding;
			this.collectorFactory = collectorFactor;
			
			this.currentIndexer = null;
		}
		
		
		
		public void setNextReader(IndexReader reader,int docBase) throws IOException {
			currentIndexer = reader;
			currentDocBase = docBase;
			for (Object o : entries.internalValues()) {
				if (o == null) continue;
				
				FacetedDateHistogramFacet.Entry e = (FacetedDateHistogramFacet.Entry)o;
				
				e.collector.setNextReader(reader, docBase);
				
			}
		}

		public void onValue(int docId, long value) {
			long time = tzRounding.calc(value);
			
			FacetedDateHistogramFacet.Entry entry;
			entry = getOrCreateEntry(value, time);
			
			try {
				entry.collector.collect(docId);
			} catch (Exception e) {
				throw new RuntimeException("Error creating an internal collector",e);
			}			

		}

		private FacetedDateHistogramFacet.Entry getOrCreateEntry(
				long value, long time) {
			FacetedDateHistogramFacet.Entry entry;
			entry = entries.get(time);
			if (entry == null) {
				try {
					entry = new FacetedDateHistogramFacet.Entry(time,collectorFactory.createInternalCollector());
					entry.collector.setNextReader(currentIndexer, currentDocBase);
				} catch (Exception e) {
					throw new RuntimeException("Error creating an internal collector",e);
				}
				
		
				entries.put(time, entry);
			}
			return entry;
		}
	
		
	



	}
}