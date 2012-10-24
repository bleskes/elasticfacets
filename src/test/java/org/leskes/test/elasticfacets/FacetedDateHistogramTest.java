package org.leskes.test.elasticfacets;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;

import java.io.UnsupportedEncodingException;
import java.util.List;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.search.facet.terms.TermsFacet;
import org.leskes.elasticfacets.FacetedDateHistogramFacet;
import org.leskes.elasticfacets.FacetedDateHistogramFacet.Entry;
import org.testng.annotations.Test;

public class FacetedDateHistogramTest extends AbstractFacetTest {

	@Override
	protected void loadData() throws Exception {

		client.prepareIndex("test", "type1")
				.setSource(
						jsonBuilder().startObject().field("tag", "week1").field("date","2012-07-03T10:00:00.000Z")
								.endObject()).execute().actionGet();
		client.admin().indices().prepareFlush().setRefresh(true).execute()
				.actionGet();
		
		documentCount++;

		client.prepareIndex("test", "type1")
				.setSource(
						jsonBuilder().startObject().field("tag", "week2").field("date","2012-07-10T10:00:00.000Z")
								.endObject()).execute().actionGet();

		client.admin().indices().prepareRefresh().execute().actionGet();
		documentCount++;

	}
	
	@Test
	public void SimpleWeekIntervalTest() throws Exception{
		for (int i = 0; i < numberOfRuns(); i++) {
			SearchResponse searchResponse = client
					.prepareSearch()
					.setSearchType(SearchType.COUNT)
					.setFacets(
							("{ \"facet1\": { \"faceted_date_histogram\" : " +
									"{ \"field\": \"date\", \"size\": 2 ,\"interval\": \"week\", "+
						     "            \"facet\": { \"terms\" : { \"field\": \"tag\"}}  "+
							 "}      }      }"
								).getBytes("UTF-8"))
					.execute().actionGet();

			assertThat(searchResponse.hits().totalHits(), equalTo(documentCount));

			FacetedDateHistogramFacet facet = searchResponse.facets().facet("facet1");
			assertThat(facet.name(), equalTo("facet1"));
			List<Entry> entries = facet.collapseToAList();
			assertThat(entries.size(), equalTo(2));
			assertThat(entries.get(0).time,equalTo(1341187200000L));
			assertThat(((TermsFacet)entries.get(0).facet()).getEntries().get(0).getTerm(), equalTo("week1"));
			assertThat(entries.get(1).time,equalTo(1341792000000L));
			assertThat(((TermsFacet)entries.get(1).facet()).getEntries().get(0).getTerm(), equalTo("week2"));
		}
	}

}
