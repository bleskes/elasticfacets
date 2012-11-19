package org.leskes.test.elasticfacets.facets;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.settings.ImmutableSettings.Builder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.search.facet.terms.TermsFacet;
import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;

public class TokenizerHashedStringFacetTest extends AbstractFacetTest {

	@Override
	protected void loadData() throws Exception {
		
	 client.admin().indices().preparePutMapping("test").setType("type1").setSource("{\"type1\":{"+
    "  \"properties\" : {          "+
    "       \"tag\" : {\"type\" : \"string\", \"analyzer\" : \"hsf_analyzer\"} "+
    "    } "+
    "}}"
    ).execute().actionGet();

		client.prepareIndex("test", "type1")
				.setSource("{ \"tag\" : \"1s2s3\"}").execute().actionGet();
		client.admin().indices().prepareFlush().setRefresh(true).execute()
				.actionGet();

		client.prepareIndex("test", "type1")
				.setSource("{ \"tag\" : \"2s4\"}").execute().actionGet();

		client.admin().indices().prepareRefresh().execute().actionGet();
	}
	
	@Override
	protected void configureNodeSettings(Builder settingsBuilder) {
		super.configureNodeSettings(settingsBuilder);
		settingsBuilder.put("index.analysis.analyzer.hsf_analyzer.type", "pattern");
		settingsBuilder.put("index.analysis.analyzer.hsf_analyzer.pattern", "s");
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
			
			logFacet(facet);
			
			assertThat(facet.entries().size(), equalTo(4));
			assertThat(facet.entries().get(0).term(),equalTo("2"));
			assertThat(facet.entries().get(0).count(), equalTo(2));
			
			assertThat(facet.entries().get(1).term(),
					anyOf(equalTo("1"), equalTo("3"),equalTo("4")));
			assertThat(facet.entries().get(1).count(), equalTo(1));
			
			assertThat(facet.entries().get(2).term(),
					anyOf(equalTo("1"), equalTo("3"),equalTo("4")));
			assertThat(facet.entries().get(2).count(), equalTo(1));
			
			assertThat(facet.entries().get(3).term(),
					anyOf(equalTo("1"), equalTo("3"),equalTo("4")));
			assertThat(facet.entries().get(3).count(), equalTo(1));
		}
	}

}
