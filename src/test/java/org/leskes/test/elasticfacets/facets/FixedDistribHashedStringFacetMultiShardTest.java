package org.leskes.test.elasticfacets.facets;

import org.testng.annotations.Test;

@Test
public class FixedDistribHashedStringFacetMultiShardTest extends FixedDistribHashedStringFacetTest {

	@Override
	protected int numberOfShards() {
		return 5;
	}
	
	
}
