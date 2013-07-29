package io.tilde.moneta;

import com.datastax.driver.core.Cluster;

/**
 * Configures and builds an instance of MonetaMapper.
 *
 * @author Carl Lerche
 */
public class MonetaConfig {

  private Cluster cluster;

  private String keyspace;

  public MonetaConfig withCluster(Cluster cluster) {
    this.cluster = cluster;
    return this;
  }

  public MonetaConfig withKeyspace(String val) {
    keyspace = val;
    return this;
  }

  public MonetaMapper connect() {
    return new MonetaMapper(getOrBuildCluster().connect(), keyspace);
  }

  private Cluster getOrBuildCluster() {
    if (cluster == null) {
      cluster = Cluster.builder()
        .addContactPoint("127.0.0.1").build();
    }

    return cluster;
  }
}