package org.leskes.test.elasticfacets;

import org.testng.annotations.Test;

@Test
public class FixedDistribHashedStringFacetMultiShardTest extends FixedDistribHashedStringFacetTest {

	@Override
	protected int numberOfShards() {
		return 5;
	}
	
	
}
