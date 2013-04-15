package org.leskes.elasticfacets;

import org.elasticsearch.action.ActionModule;
import org.elasticsearch.common.inject.Binder;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.rest.RestModule;
import org.elasticsearch.search.facet.FacetModule;
import org.leskes.elasticfacets.cache.CacheStatsPerFieldAction;
import org.leskes.elasticfacets.cache.RestCacheStatsPerFieldAction;
import org.leskes.elasticfacets.cache.TransportCacheStatsPerFieldAction;
import org.leskes.elasticfacets.fields.HashedStringFieldSettings;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;


public class ElasticFacetsPlugin extends AbstractPlugin  {
	
	final static ESLogger logger = Loggers.getLogger(ElasticFacetsPlugin.class);

	public ElasticFacetsPlugin() {
		logger.info("ElasticFacets plugin initialized");
    }

	public String name() {
		return "ElasticFacetsPlugin";
	}

   @Override
   public Collection<Module> shardModules(Settings settings) {
      List<Module> ret = new ArrayList<Module>();
      ret.add(new Module() {
         @Override
         public void configure(Binder binder) {
            binder.bind(HashedStringFieldSettings.class).asEagerSingleton();
         }
      });
      return ret;
   }

   public String description() {
		return "A plugin adding the Faceted Date Histogram facet type.";
	}

   @Override
    public void processModule(Module module) {
		if (module instanceof FacetModule) {
	    	((FacetModule)module).addFacetProcessor(FacetedDateHistogramFacetProcessor.class);
	    	((FacetModule)module).addFacetProcessor(HashedStringsFacetProcessor.class);
		}
        if (module instanceof ActionModule) {
            ((ActionModule)module).registerAction(CacheStatsPerFieldAction.INSTANCE, TransportCacheStatsPerFieldAction.class);
        }
        if (module instanceof RestModule) {
            ((RestModule)module).addRestAction(RestCacheStatsPerFieldAction.class);
        }
    }


}
