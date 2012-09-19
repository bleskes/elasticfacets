package org.elasticsearch.plugin.elasticfacets;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.index.analysis.AnalysisModule;
import org.elasticsearch.index.query.IndexQueryParserModule;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.rest.RestModule;
import org.elasticsearch.search.facet.FacetModule;
import org.hamcrest.core.IsInstanceOf;


public class ElasticFacetsPlugin extends AbstractPlugin  {
	
	final static ESLogger logger = Loggers.getLogger(ElasticFacetsPlugin.class);

	@Inject public ElasticFacetsPlugin() {
		logger.info("ElasticFacets plugin initialized");
    }

	public String name() {
		return "ElasticFacetsPlugin";
	}


	public String description() {
		return "example plugin ES meetup";
	}
	
	@Override
    public void processModule(Module module) {

		if (module instanceof FacetModule) {
			logger.debug("Registering the LetterCounterFacetProcessor");
	    	((FacetModule)module).addFacetProcessor(LetterCountFacetProcessor.class);
	    	((FacetModule)module).addFacetProcessor(FacetedDateHistogramFacetProcessor.class);
	    	
		}
    }

}
