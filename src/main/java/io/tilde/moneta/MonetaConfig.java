package io.tilde.moneta;

import com.datastax.driver.core.Cluster;

/**
 * Configures and builds an instance of MonetaMapper.
 *
 * @author Carl Lerche
 */
public class MonetaConfig {

  private String keyspace;

  public MonetaConfig withKeyspace(String val) {
    keyspace = val;
    return this;
  }

  public MonetaMapper connect() {
    Cluster cluster = Cluster.builder()
      .addContactPoint("127.0.0.1").build();

    return new MonetaMapper(cluster.connect(), keyspace);
  }
}