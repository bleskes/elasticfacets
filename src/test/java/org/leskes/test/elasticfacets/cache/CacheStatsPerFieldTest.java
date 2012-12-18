package org.leskes.test.elasticfacets.cache;

import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.leskes.elasticfacets.cache.CacheStatsPerFieldAction;
import org.leskes.elasticfacets.cache.CacheStatsPerFieldRequest;
import org.leskes.elasticfacets.cache.CacheStatsPerFieldResponse;
import org.leskes.elasticfacets.cache.CacheStatsPerFieldStats;
import org.leskes.test.elasticfacets.utils.AbstractNodesTests;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class CacheStatsPerFieldTest extends AbstractNodesTests {
    protected long documentCount = 0;

   protected void loadData() throws IOException {
      for (int i=0;i<100;i++) {
          client.prepareIndex("test", "type1")
                  .setSource(
                          jsonBuilder().startObject().field("tag", "tag" + i)
                                  .endObject()).execute().actionGet();
          documentCount++;
      }
   }


   protected void configureNodeSettings(ImmutableSettings.Builder settingsBuilder) {
        settingsBuilder.put("index.number_of_shards", numberOfShards())
                .put("index.number_of_replicas", 0);
    }


    protected int numberOfShards() {
        return super.numberOfShards();
    }

    protected int numberOfNodes() {
        return super.numberOfShards();
    }

    protected int numberOfRuns() {
        return 5;
    }

    @BeforeMethod
    public void clearCache() throws ExecutionException, InterruptedException {
        client.admin().indices().prepareClearCache().execute().get();
    }

    @Test
    public void TestEmpty() throws ExecutionException, InterruptedException {
        CacheStatsPerFieldResponse r = client.admin().cluster()
                .execute(CacheStatsPerFieldAction.INSTANCE, new CacheStatsPerFieldRequest())
                .get();

        for (CacheStatsPerFieldStats s : r) {
            assertThat(s.fieldEntries().size(), equalTo(0));
        }
    }

    @Test
    public void TestNotEmptyAfterFacet() throws Exception {
        for (int i = 0; i < numberOfRuns(); i++) {
            clearCache();
            facetOnTags();


            CacheStatsPerFieldResponse r = client.admin().cluster()
                    .execute(CacheStatsPerFieldAction.INSTANCE, new CacheStatsPerFieldRequest())
                    .get();

            for (CacheStatsPerFieldStats s : r) {
                assertThat(s.fieldEntries().size(), equalTo(1));
                assertThat(s.fieldEntries().get(0).fieldName, equalTo("tag"));
                assertThat(s.fieldEntries().get(0).size, greaterThan(0L));
            }
        }
    }

    @Test
    public void TestJSONResponse() throws Exception {

        facetOnTags();

        CacheStatsPerFieldResponse r = client.admin().cluster()
                .execute(CacheStatsPerFieldAction.INSTANCE, new CacheStatsPerFieldRequest())
                .get();

        XContentBuilder builder = jsonBuilder();
        builder.startObject();
        r.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        builder.close();

        String JSON = builder.bytes().toUtf8();
        logger.info("JSON: {}",JSON);
        // {"cluster_name":"test-cluster-boazmbp.fritz.box",
        //      "nodes":{"qpCAo38-Rm2epLPs0FJu8w":
        //          {"setTimestamp":1353104133685,"name":"node0",
        //           "transport_address":"inet[/192.168.1.107:9300]","hostname":"boazmbp.fritz.box",
        //                 "fields":{"tag":{"size":144}}}}}

    }


    protected void facetOnTags() throws UnsupportedEncodingException {
        client
                .prepareSearch()
                .setSearchType(SearchType.COUNT)
                .setFacets(
                        ("{ \"facet1\": { \"terms\" : { \"field\": \"tag\" } } }"
                        ).getBytes("UTF-8"))
                .execute().actionGet();
    }



}
