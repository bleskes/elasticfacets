package org.leskes.test.elasticfacets;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.facet.terms.TermsFacet;
import org.leskes.elasticfacets.HashedStringFacetProcessor;
import org.leskes.elasticfacets.fields.HashedStringFieldType;
import org.leskes.test.elasticfacets.utils.AbstractNodesTests;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 *
 */
public class FixedDistribHashedStringFacetTest extends AbstractNodesTests {

	private Client client;
	private long documentCount =0;
	
	final static ESLogger logger = Loggers
			.getLogger(FixedDistribHashedStringFacetTest.class);

	@BeforeClass
	public void createNodes() throws Exception {
		Settings settings = ImmutableSettings.settingsBuilder()
				.put("index.number_of_shards", numberOfShards())
				.put("index.number_of_replicas", 0).build();
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
		
		int max_term_count = maxTermCount();
		
		client.prepareIndex("test", "type1")
		.setSource("{ \"otherfield\" : 1 }")
		.execute().actionGet();
		documentCount++;
		
		for (int i=1;i<=max_term_count;i++) {
			for (int j=0;j<i;j++) {
				assertThat(String.format("Checking has code of %s & %s",getTerm(i),getTerm(j)),
						HashedStringFieldType.hashCode(getTerm(i)), not(equalTo(HashedStringFieldType.hashCode(getTerm(j)))));
				client.prepareIndex("test", "type1")
				.setSource(String.format("{ \"tag\" : \"%s\"}",getTerm(i)))
				.execute().actionGet();
				documentCount++;
			}
		}
		
		client.admin().indices().prepareRefresh().execute().actionGet();

	}
	
	protected String getTerm(int i) {
		return String.format("term_%s",i);
	}
	
	protected int getFacetSize() {
		return 100;
	}

	
	
	protected int maxTermCount() {
		return 100;
	}

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
			
			logger.info("Evaluating pos={}: term={} count={}", facet_pos,
					facet.entries().get(facet_pos).term(),facet.entries().get(facet_pos).count());
		}

	}
	
	@Test
	public void SimpleCallTest() throws Exception {
		for (int i = 0; i < numberOfRuns(); i++) {
			SearchResponse searchResponse = client
					.prepareSearch()
					.setSearchType(SearchType.COUNT)
					.setFacets(
							String.format("{ \"facet1\": { \"hashed_terms\" : { \"field\": \"tag\", \"size\": %s } } }",getFacetSize())
								.getBytes("UTF-8"))
					.execute().actionGet();

			assertThat(searchResponse.hits().totalHits(), equalTo(documentCount));

			TermsFacet facet = searchResponse.facets().facet("facet1");
			assertThat(facet.name(), equalTo("facet1"));
			assertThat(facet.entries().size(), equalTo(getFacetSize()));
			//assertThat(facet.totalCount(),equalTo(documentCount)); 
			assertThat(facet.missingCount(),equalTo(1L)); // one missing doc.

			for (int term=maxTermCount()-getFacetSize()+1;term<=maxTermCount();term++) {
				int facet_pos = maxTermCount()-term;
				
				assertThat(facet.entries().get(facet_pos).term(),equalTo(getTerm(term)));
				assertThat(facet.entries().get(facet_pos).count(),equalTo(term));
			}
		}
	}

	
	
	@Test
	public void OutputScriptTest() throws Exception {
		int facet_size = 10;
		for (int i = 0; i < numberOfRuns(); i++) {
			SearchResponse searchResponse = client
					.prepareSearch()
					.setSearchType(SearchType.COUNT)
					.setFacets(
							String.format("{ \"facet1\": { \"hashed_terms\" : " +
									"{ \"field\": \"tag\", \"size\": %s ,\"output_script\" : \"_source.tag+'s'\" } } }",
									facet_size)
								.getBytes("UTF-8"))
					.execute().actionGet();

			assertThat(searchResponse.hits().totalHits(), equalTo(documentCount));

			TermsFacet facet = searchResponse.facets().facet("facet1");
			assertThat(facet.name(), equalTo("facet1"));
			assertThat(facet.entries().size(), equalTo(facet_size));
			//assertThat(facet.totalCount(),equalTo(documentCount)); 
			assertThat(facet.missingCount(),equalTo(1L)); // one missing doc.

			for (int term=maxTermCount()-facet_size+1;term<=maxTermCount();term++) {
				int facet_pos = maxTermCount()-term;
				
				assertThat(facet.entries().get(facet_pos).term(),equalTo(getTerm(term)+"s"));
				assertThat(facet.entries().get(facet_pos).count(),equalTo(term));
			}
		}
	}

	
	@Test
	public void ExcludeTest() throws Exception {
		// exclude the top most terms
		for (int i = 0; i < numberOfRuns(); i++) {
			SearchResponse searchResponse = client
					.prepareSearch()
					.setSearchType(SearchType.COUNT)
					.setFacets(
							String.format("{ \"facet1\": { \"hashed_terms\" : " +
									"{ \"field\": \"tag\", \"size\": %s , \"exclude\": [ \"%s\" , \"%s\"] } } }",
									10,getTerm(maxTermCount()),getTerm(maxTermCount()-1))
								.getBytes("UTF-8"))
					.execute().actionGet();

			assertThat(searchResponse.hits().totalHits(), equalTo(documentCount));

			TermsFacet facet = searchResponse.facets().facet("facet1");
			assertThat(facet.name(), equalTo("facet1"));
			assertThat(facet.entries().size(), equalTo(10));
			//assertThat(facet.totalCount(),equalTo(documentCount)); 
			assertThat(facet.missingCount(),equalTo(1L)); // one missing doc.
			
			int maxTermInFacet = maxTermCount()-2;

			for (int term=maxTermInFacet-10+1;term<=maxTermInFacet-2;term++) {
				int facet_pos = maxTermInFacet-term;
				
				assertThat(facet.entries().get(facet_pos).term(),equalTo(getTerm(term)));
				assertThat(facet.entries().get(facet_pos).count(),equalTo(term));
			}
		}
	}

}
