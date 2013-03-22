package org.leskes.test.elasticfacets.facets;

import org.testng.annotations.Test;

@Test
public class HashedStringFacetMultiShardFixedDistribTest extends HashedStringFacetFixedDistribTest {

	@Override
	protected int numberOfShards() {
		return 5;
	}
	
	
}
