package org.leskes.test.elasticfacets;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;

import java.io.IOException;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.search.facet.terms.TermsFacet;
import org.leskes.test.elasticfacets.utils.AbstractNodesTests;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 *
 */
public class SimpleHashedStringFacetTest extends HashedStringFacetTest {

	private Client client;

	
	@Test
	public void SimpleHashStringFacet() throws Exception {

		for (int i = 0; i < numberOfRuns(); i++) {
			SearchResponse searchResponse = client
					.prepareSearch()
					.setSearchType(SearchType.COUNT)
					.setFacets(
							XContentFactory.jsonBuilder().startObject()
									.startObject("facet1")
									.startObject("hashed_terms")
									.field("field", "tag").endObject()
									.endObject().endObject().bytes()).execute()
					.actionGet();

			assertThat(searchResponse.hits().totalHits(), equalTo(2l));
			assertThat(searchResponse.hits().hits().length, equalTo(0));
			TermsFacet facet = searchResponse.facets().facet("facet1");
			assertThat(facet.name(), equalTo("facet1"));
			assertThat(facet.entries().size(), equalTo(2));
			assertThat(facet.entries().get(0).term(),
					anyOf(equalTo("green"), equalTo("blue")));
			assertThat(facet.entries().get(0).count(), equalTo(1));
			assertThat(facet.entries().get(1).term(),
					anyOf(equalTo("green"), equalTo("blue")));
			assertThat(facet.entries().get(1).count(), equalTo(1));
		}
	}


	@Override
	protected void loadData() throws Exception  {
	
		client.prepareIndex("test", "type1")
				.setSource(
						jsonBuilder().startObject().field("tag", "green")
								.endObject()).execute().actionGet();
		client.admin().indices().prepareFlush().setRefresh(true).execute()
				.actionGet();

		client.prepareIndex("test", "type1")
				.setSource(
						jsonBuilder().startObject().field("tag", "blue")
								.endObject()).execute().actionGet();

		client.admin().indices().prepareRefresh().execute().actionGet();

	}

}
