package org.leskes.test.elasticfacets.facets;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.search.facet.terms.TermsFacet;
import org.leskes.elasticfacets.HashedStringsFacet;
import org.leskes.elasticfacets.fields.HashedStringFieldType;
import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.equalToIgnoringCase;
import static org.hamcrest.Matchers.isEmptyOrNullString;

/**
 *
 */
public class HashedStringsFacetFixedDistribTest extends AbstractFacetTest {
   protected long documentCount =0;
   protected long fieldTokenCount=0;

   protected void loadData() {
		int max_term_count = maxTermCount();
		
		client.prepareIndex("test", "type1")
		.setSource("{ \"otherfield\" : 1 }")
		.execute().actionGet();
		documentCount++;
		
		for (int i=1;i<=max_term_count;i++) {
			for (int j=0;j<i;j++) {
				client.prepareIndex("test", "type1")
				.setSource(String.format("{ \"tag\" : \"%s\"}",getTerm(i,j%2 == 0)))
				.execute().actionGet();
				documentCount++;
            fieldTokenCount++;
			}
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
			assertThat(facet.totalCount(),equalTo(fieldTokenCount));
			assertThat(facet.missingCount(),equalTo(1L)); // one missing doc.

			for (int term=maxTermCount()-getFacetSize()+1;term<=maxTermCount();term++) {
				int facet_pos = maxTermCount()-term;
				
				assertThat(facet.entries().get(facet_pos).term(),equalToIgnoringCase(getTerm(term)));
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
									"{ \"field\": \"tag\", \"size\": %s ,\"fetch_size\" : %s ,\"output_script\" : \"_source.tag.toLowerCase()+'s'\" } } }",
									facet_size,maxTermCount())
								.getBytes("UTF-8"))
					.execute().actionGet();

			assertThat(searchResponse.hits().totalHits(), equalTo(documentCount));

         TermsFacet facet = searchResponse.facets().facet("facet1");
			logFacet(facet);
			assertThat(facet.name(), equalTo("facet1"));
			assertThat(facet.entries().size(), equalTo(facet_size));
         assertThat(facet.totalCount(),equalTo(fieldTokenCount));
			assertThat(facet.missingCount(),equalTo(1L)); // one missing doc.

			for (int term=maxTermCount()-facet_size+1;term<=maxTermCount();term++) {
				int facet_pos = maxTermCount()-term;
				
				assertThat(facet.entries().get(facet_pos).term(),equalTo(getTerm(term,true)+"s"));
				assertThat(facet.entries().get(facet_pos).count(),equalTo(term));
			}
		}
	}

	
	@Test
	public void ExcludeTest() throws Exception {
		// exclude the top most terms
		int facet_size = 10;

		for (int i = 0; i < numberOfRuns(); i++) {
			SearchResponse searchResponse = client
					.prepareSearch()
					.setSearchType(SearchType.COUNT)
					.setFacets(
							String.format("{ \"facet1\": { \"hashed_terms\" : " +
									"{ \"field\": \"tag\", \"size\": %s,\"fetch_size\" : %s , \"exclude\": [ \"%s\" , %s] } } }",
									facet_size,maxTermCount(), getTerm(maxTermCount()),
                             HashedStringFieldType.hashCode(getTerm(maxTermCount() - 1)))
								.getBytes("UTF-8"))
					.execute().actionGet();

			assertThat(searchResponse.hits().totalHits(), equalTo(documentCount));

			TermsFacet facet = searchResponse.facets().facet("facet1");
			assertThat(facet.name(), equalTo("facet1"));
			assertThat(facet.entries().size(), equalTo(facet_size));
         assertThat(facet.totalCount(),equalTo(fieldTokenCount));
         assertThat(facet.missingCount(),equalTo(1L)); // one missing doc.
			
			int maxTermInFacet = maxTermCount()-2;

			for (int term=maxTermInFacet-facet_size+1;term<=maxTermInFacet-2;term++) {
				int facet_pos = maxTermInFacet-term;
				
				assertThat(facet.entries().get(facet_pos).term(),equalToIgnoringCase(getTerm(term)));
				assertThat(facet.entries().get(facet_pos).count(),equalTo(term));
			}
		}
	}

   @Test
   public void IncludeTest() throws Exception {
      int facet_size = 10;

      for (int i = 0; i < numberOfRuns(); i++) {
         SearchResponse searchResponse = client
                 .prepareSearch()
                 .setSearchType(SearchType.COUNT)
                 .setFacets(
                         String.format("{ \"facet1\": { \"hashed_terms\" : " +
                                 "{ \"field\": \"tag\", \"size\": %s,\"fetch_size\" : %s , \"include\": [ \"%s\" , %s] } } }",
                                 facet_size,maxTermCount(), getTerm(maxTermCount()),
                                 HashedStringFieldType.hashCode(getTerm(maxTermCount() - 1)))
                                 .getBytes("UTF-8"))
                 .execute().actionGet();

         assertThat(searchResponse.hits().totalHits(), equalTo(documentCount));

         TermsFacet facet = searchResponse.facets().facet("facet1");
         assertThat(facet.name(), equalTo("facet1"));
         assertThat(facet.entries().size(), equalTo(2));
         assertThat(facet.totalCount(),equalTo(fieldTokenCount));
         assertThat(facet.missingCount(),equalTo(1L)); // one missing doc.

         assertThat(facet.entries().get(0).term(),equalToIgnoringCase(getTerm(maxTermCount())));
         assertThat(facet.entries().get(0).count(),equalTo(maxTermCount()));
         assertThat(facet.entries().get(1).term(),equalToIgnoringCase(getTerm(maxTermCount()-1)));
         assertThat(facet.entries().get(1).count(),equalTo(maxTermCount()-1));

      }
   }

   @Test
   public void OutputModeHashTest() throws Exception {
      int facet_size = 10;

      for (int i = 0; i < numberOfRuns(); i++) {
         SearchResponse searchResponse = client
                 .prepareSearch()
                 .setSearchType(SearchType.COUNT)
                 .setFacets(
                         String.format("{ \"facet1\": { \"hashed_terms\" : " +
                                 "{ \"field\": \"tag\", \"size\": %s,\"fetch_size\" : %s, output_mode: \"hash\" } } }",
                                 facet_size,maxTermCount(), getTerm(maxTermCount())
                                 )
                                 .getBytes("UTF-8"))
                 .execute().actionGet();

         assertThat(searchResponse.hits().totalHits(), equalTo(documentCount));

         HashedStringsFacet facet = searchResponse.facets().facet("facet1");
         assertThat(facet.name(), equalTo("facet1"));
         assertThat(facet.entries().size(), equalTo(facet_size));
         assertThat(facet.entries().get(0).term(),isEmptyOrNullString());
         assertThat(facet.entries().get(0).getTermHash(),
                 equalTo(HashedStringFieldType.hashCode(getTerm(maxTermCount()))));
         assertThat(facet.entries().get(0).count(),equalTo(maxTermCount()));
         assertThat(facet.entries().get(1).term(), isEmptyOrNullString());
         assertThat(facet.entries().get(1).getTermHash(),
                 equalTo(HashedStringFieldType.hashCode(getTerm(maxTermCount()-1))));
         assertThat(facet.entries().get(1).count(),equalTo(maxTermCount()-1));

      }
   }


}
