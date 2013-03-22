package org.leskes.test.elasticfacets.facets;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.search.facet.terms.TermsFacet;
import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

/**
 *
 */
public class HashedStringFacetMultiTermsTest extends AbstractFacetTest {

   protected long documentCount =0;
	
	@Test
	public void MaxDocTermsTest() throws Exception {

		for (int i = 0; i < numberOfRuns(); i++) {
			SearchResponse searchResponse = client
					.prepareSearch()
					.setSearchType(SearchType.COUNT)
					.setFacets(
							XContentFactory.jsonBuilder().startObject()
									.startObject("facet1")
									.startObject("hashed_terms")
									.field("field", "tags?max_doc_terms=10").endObject()
									.endObject().endObject().bytes()).execute()
					.actionGet();

			assertThat(searchResponse.hits().totalHits(), equalTo(documentCount));
			assertThat(searchResponse.hits().hits().length, equalTo(0));
			TermsFacet facet = searchResponse.facets().facet("facet1");
			assertThat(facet.name(), equalTo("facet1"));
			assertThat(facet.entries().size(), equalTo(10));
			assertThat(facet.entries().get(0).term(),equalTo("term_0"));
			assertThat(facet.entries().get(0).count(), equalTo(10));
			assertThat(facet.entries().get(1).term(),equalTo("term_1"));
			assertThat(facet.entries().get(1).count(), equalTo(9));
		}
	}

   @Test
   public void MaxTermDocsTest() throws Exception {

      for (int i = 0; i < numberOfRuns(); i++) {
         SearchResponse searchResponse = client
                 .prepareSearch()
                 .setSearchType(SearchType.COUNT)
                 .setFacets(
                         XContentFactory.jsonBuilder().startObject()
                                 .startObject("facet1")
                                 .startObject("hashed_terms")
                                 .field("field", "tags?max_term_docs=10").endObject()
                                 .endObject().endObject().bytes()).execute()
                 .actionGet();

         assertThat(searchResponse.hits().totalHits(), equalTo(documentCount));
         assertThat(searchResponse.hits().hits().length, equalTo(0));
         TermsFacet facet = searchResponse.facets().facet("facet1");
         assertThat(facet.name(), equalTo("facet1"));
         assertThat(facet.entries().size(), equalTo(10));
         assertThat(facet.entries().get(0).term(),equalTo("term_90"));
         assertThat(facet.entries().get(0).count(), equalTo(10));
         assertThat(facet.entries().get(1).term(),equalTo("term_91"));
         assertThat(facet.entries().get(1).count(), equalTo(9));
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
         sb.deleteCharAt(sb.length()-1);
         sb.append("]");

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

   protected int getFacetSize() {
      return 100;
   }



   protected int maxTermCount() {
      return 100;
   }

}
