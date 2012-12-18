
// Base class for test that should interact with multiple notdes. STOLEN FROM ELASTIC SEARCH

package org.leskes.test.elasticfacets.utils;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.network.NetworkUtils;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.common.settings.ImmutableSettings.Builder.EMPTY_SETTINGS;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

public abstract class AbstractNodesTests {

    protected final ESLogger logger = Loggers.getLogger(getClass());
   protected Client client;

   private Map<String, Node> nodes = new HashMap<String, Node>();

    private Map<String, Client> clients = new HashMap<String, Client>();

    private Settings defaultSettings = ImmutableSettings
            .settingsBuilder()
            .put("cluster.name", "test-cluster-" + NetworkUtils.getLocalAddress().getHostName())
            .build();

    public void putDefaultSettings(Settings.Builder settings) {
        putDefaultSettings(settings.build());
    }

    public void putDefaultSettings(Settings settings) {
        defaultSettings = ImmutableSettings.settingsBuilder().put(defaultSettings).put(settings).build();
    }

    public Node startNode(String id) {
        return buildNode(id).start();
    }

    public Node startNode(String id, Settings.Builder settings) {
        return startNode(id, settings.build());
    }

    public Node startNode(String id, Settings settings) {
        return buildNode(id, settings).start();
    }

    public Node buildNode(String id) {
        return buildNode(id, EMPTY_SETTINGS);
    }

    public Node buildNode(String id, Settings.Builder settings) {
        return buildNode(id, settings.build());
    }

    public Node buildNode(String id, Settings settings) {
        String settingsSource = getClass().getName().replace('.', '/') + ".yml";
        Settings finalSettings = settingsBuilder()
                .loadFromClasspath(settingsSource)
                .put(defaultSettings)
                .put(settings)
                .put("name", id)
                .build();

        if (finalSettings.get("gateway.type") == null) {
            // default to non gateway
            finalSettings = settingsBuilder().put(finalSettings).put("gateway.type", "none").build();
        }
        if (finalSettings.get("cluster.routing.schedule") != null) {
            // decrease the routing schedule so new nodes will be added quickly
            finalSettings = settingsBuilder().put(finalSettings).put("cluster.routing.schedule", "50ms").build();
        }

        Node node = nodeBuilder()
                .settings(finalSettings)
                .build();
        nodes.put(id, node);
        clients.put(id, node.client());
        return node;
    }

    public void closeNode(String id) {
        Client client = clients.remove(id);
        if (client != null) {
            client.close();
        }
        Node node = nodes.remove(id);
        if (node != null) {
            node.close();
        }
    }

    public Node node(String id) {
        return nodes.get(id);
    }

    public Client client(String id) {
        return clients.get(id);
    }

    public void closeAllNodes() {
        for (Client client : clients.values()) {
            client.close();
        }
        clients.clear();
        for (Node node : nodes.values()) {
            node.close();
        }
        nodes.clear();
    }

   @BeforeClass
   public void createNodes() throws Exception {
      ImmutableSettings.Builder settingsBuilder = settingsBuilder();
      configureNodeSettings(settingsBuilder);

      Settings settings = settingsBuilder.build();
      for (int i = 0; i < numberOfNodes(); i++) {
         startNode("node" + i, settings);
      }
      client = getClient();

      try {
         client.admin().indices().prepareDelete("test").execute()
               .actionGet();
      } catch (Exception e) {
         // ignore
      }
      client.admin().indices().prepareCreate("test").execute().actionGet();
      client.admin().cluster().prepareHealth().setWaitForGreenStatus()
            .execute().actionGet();

      loadData();

      client.admin().indices().prepareRefresh().execute().actionGet();

   }


   abstract protected void loadData() throws Exception;

   protected void configureNodeSettings(ImmutableSettings.Builder settingsBuilder) {
      settingsBuilder.put("index.number_of_shards", numberOfShards())
              .put("index.number_of_replicas", 0);
   }


   protected int numberOfNodes() {
      return numberOfShards();
   }
   protected int numberOfShards() {
      return 1;
   }

   @AfterClass
   public void closeNodes() {
      client.close();
      closeAllNodes();
   }

   protected Client getClient() {
      return client("node0");
   }
}