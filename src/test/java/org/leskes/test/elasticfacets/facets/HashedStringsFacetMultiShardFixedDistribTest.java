package org.leskes.test.elasticfacets.facets;

import org.testng.annotations.Test;

@Test
public class HashedStringsFacetMultiShardFixedDistribTest extends HashedStringsFacetFixedDistribTest {

	@Override
	protected int numberOfShards() {
		return 5;
	}

}
