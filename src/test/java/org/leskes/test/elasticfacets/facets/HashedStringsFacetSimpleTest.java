package org.leskes.test.elasticfacets.facets;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.search.facet.terms.TermsFacet;
import org.testng.annotations.Test;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;

/**
 *
 */
public class HashedStringsFacetSimpleTest extends AbstractFacetTest {

   @Override
   protected void configureNodeSettings(ImmutableSettings.Builder settingsBuilder) {
      super.configureNodeSettings(settingsBuilder);
   }

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
