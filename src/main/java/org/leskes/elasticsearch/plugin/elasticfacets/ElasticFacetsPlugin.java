package org.leskes.elasticsearch.plugin.elasticfacets;

import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.search.facet.FacetModule;


public class ElasticFacetsPlugin extends AbstractPlugin  {
	
	final static ESLogger logger = Loggers.getLogger(ElasticFacetsPlugin.class);

	public ElasticFacetsPlugin() {
		logger.info("ElasticFacets plugin initialized");
    }

	public String name() {
		return "ElasticFacetsPlugin";
	}


	public String description() {
		return "A plugin adding the Faceted Date Histogram facet type.";
	}
	
	@Override
    public void processModule(Module module) {
		if (module instanceof FacetModule) {
	    	((FacetModule)module).addFacetProcessor(FacetedDateHistogramFacetProcessor.class);
		}
    }

}
