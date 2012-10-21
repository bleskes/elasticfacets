package org.leskes.test.elasticfacets;

import org.testng.annotations.Test;

@Test
public class MultiValueFixedDistribHashedStringFacetTest extends
		FixedDistribHashedStringFacetTest {

	@Override
	protected void loadData() {
		int max_term_count = maxTermCount();
		
		client.prepareIndex("test", "type1")
		.setSource("{ \"otherfield\" : 1 }")
		.execute().actionGet();
		documentCount++;
		
		for (int i=max_term_count;i>=1;i--) {
			StringBuilder SB = new StringBuilder("[");
			SB.append("\"").append(getTerm(i,i % 2 == 0)).append("\"");
			for (int j=i+1;j<=max_term_count;j++) {
				SB.append(", \"");
				SB.append(getTerm(j,i % 2 == 0));
				SB.append("\"");
				
			}
			SB.append("]");
			
			client.prepareIndex("test", "type1")
			.setSource(String.format("{ \"tag\" : %s }",SB.toString()))
			.execute().actionGet();
			documentCount++;

		}
	}
	
    @Override
    @Test
    public void OutputScriptTest() throws Exception {
    	// not relevant (yet) in here.
    }
	
	
}
