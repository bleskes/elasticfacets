package org.leskes.test.elasticfacets;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.ImmutableSettings.Builder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.facet.terms.TermsFacet;
import org.leskes.test.elasticfacets.utils.AbstractNodesTests;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

public abstract class HashedStringFacetTest extends AbstractNodesTests {
	protected Client client;
	protected long documentCount =0;
	
	final protected ESLogger logger = Loggers
			.getLogger(getClass());

	@BeforeClass
	public void createNodes() throws Exception {
		Builder settingsBuilder = ImmutableSettings.settingsBuilder();
		configureNodeSettings(settingsBuilder);
		
		Settings settings = settingsBuilder.build();
		for (int i = 0; i < numberOfNodes(); i++) {
			startNode("node" + i, settings);
		}
		client = getClient();
		
		try {
			client.admin().indices().prepareDelete("test").execute()
					.actionGet();
		} catch (Exception e) {
			// ignore
		}
		client.admin().indices().prepareCreate("test").execute().actionGet();
		client.admin().cluster().prepareHealth().setWaitForGreenStatus()
				.execute().actionGet();
		
		loadData();
		
		client.admin().indices().prepareRefresh().execute().actionGet();

	}


	protected void configureNodeSettings(Builder settingsBuilder) {
		settingsBuilder.put("index.number_of_shards", numberOfShards())
				.put("index.number_of_replicas", 0);
	}

	
	abstract protected void loadData() throws Exception;



	protected int numberOfShards() {
		return 1;
	}

	protected int numberOfNodes() {
		return 1;
	}

	protected int numberOfRuns() {
		return 5;
	}

	@AfterClass
	public void closeNodes() {
		client.close();
		closeAllNodes();
	}

	protected Client getClient() {
		return client("node0");
	}
	
	protected void logFacet(TermsFacet facet) {
		for (int facet_pos=0;facet_pos<facet.entries().size();facet_pos++) {
			
			logger.debug("Evaluating pos={}: term={} count={}", facet_pos,
					facet.entries().get(facet_pos).term(),facet.entries().get(facet_pos).count());
		}

	}

}
