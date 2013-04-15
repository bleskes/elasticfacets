package org.leskes.test.elasticfacets.facets;

import org.elasticsearch.action.admin.indices.cache.clear.ClearIndicesCacheRequest;
import org.elasticsearch.action.admin.indices.settings.UpdateSettingsRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.search.facet.terms.TermsFacet;
import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

/**
 *
 */
public class HashedStringsFacetMultiTermsTest extends AbstractFacetTest {

   protected long documentCount =0;

   @Override
   protected void configureNodeSettings(ImmutableSettings.Builder settingsBuilder) {
      super.configureNodeSettings(settingsBuilder);
   }

   @Test
	public void DynamicSettingsTest() throws Exception {

		for (int i = 0; i < numberOfRuns(); i++) {

         // IMPORTANT _ excluded terms do not count for ANYTHING, also not doc term counts..

         String settingsSource = XContentFactory.jsonBuilder().startObject()
                 .startObject("hashed_strings").startObject("field").startObject("tags")
                 .field("max_terms_per_doc", 10 + i)
                 .startArray("exclude").value("ignoredterm").endArray()
                 .field("exclude_regex","ignoredterm\\d")
                 .endObject().endObject().endObject().string();
         Settings settings = ImmutableSettings.settingsBuilder().loadFromSource(settingsSource).build();
         client.admin().indices().updateSettings(new UpdateSettingsRequest(settings,"test"))
                 .actionGet();

         client.admin().indices().clearCache(new ClearIndicesCacheRequest()).actionGet();

			SearchResponse searchResponse = client
					.prepareSearch()
					.setSearchType(SearchType.COUNT)
					.setFacets(
                       XContentFactory.jsonBuilder().startObject()
                               .startObject("facet1")
                               .startObject("hashed_terms")
                               .field("field", "tags")
                               .field("size", maxTermCount()).endObject()
                               .endObject().endObject().bytes()).execute()
					.actionGet();

			assertThat(searchResponse.hits().totalHits(), equalTo(documentCount));
			assertThat(searchResponse.hits().hits().length, equalTo(0));
			TermsFacet facet = searchResponse.facets().facet("facet1");
			assertThat(facet.name(), equalTo("facet1"));
			assertThat(facet.entries().size(), equalTo(10 + i));
			assertThat(facet.entries().get(0).term(),equalTo("term_0"));
			assertThat(facet.entries().get(0).count(), equalTo(10+i));
			assertThat(facet.entries().get(1).term(),equalTo("term_1"));
			assertThat(facet.entries().get(1).count(), equalTo(9+i));
		}
   }

   @Override
	protected void loadData() throws Exception  {

      int max_term_count = maxTermCount();

      client.prepareIndex("test", "type1")
              .setSource("{ \"otherfield\" : 1 }")
              .execute().actionGet();
      documentCount++;

      for (int i=1;i<=max_term_count;i++) {
         StringBuilder sb = new StringBuilder("[");
         for (int j=0;j<i;j++) {
            sb.append("\"").append(getTerm(j,i%2 == 0)).append("\",");
         }
         sb.append(" \"ignoredTerm\", \"ignoredTerm2\" ]");

         client.prepareIndex("test", "type1")
                 .setSource(String.format("{ \"tags\" : %s }", sb.toString()))
                 .execute().actionGet();
         documentCount++;
      }
	}

   protected String getTerm(int i) {
      return getTerm(i,true);
   }

   protected String getTerm(int i,boolean lowerCase) {
      return String.format(lowerCase ? "term_%s" : "Term_%s",i);
   }



   protected int maxTermCount() {
      return 100;
   }

}
