package io.tilde.moneta;

import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Session;
import org.junit.Before;

public class TestCase {

  private MonetaMapper mapper = null;

  public String keyspace() {
    return "moneta";
  }

  public MonetaMapper mapper() {
    if (mapper == null) {
      mapper = MonetaMapper.configure()
        .withKeyspace(keyspace())
        .connect();
    }

    return mapper;
  }

  public Session session() {
    return mapper().getSession();
  }

  @Before
  public void reset() {
    createKeyspace(keyspace());

    createTable("songs",
      "id uuid PRIMARY KEY, " +
      "name text, " +
      "explicit boolean, " +
      "plays varint");
  }

  public void createKeyspace(String name) {
    for (KeyspaceMetadata ks : session().getCluster().getMetadata().getKeyspaces()) {
      if (name.equals(ks.getName())) {
        session().execute("DROP KEYSPACE " + name);
      }
    }

    session().execute(
      "CREATE KEYSPACE " + name + " WITH replication " +
        "= {'class': 'SimpleStrategy', 'replication_factor':3};");
  }

  public void createTable(String name, String props) {
    createTable(name, keyspace(), props);
  }

  public void createTable(String name, String keyspace, String props) {
    session().execute("CREATE TABLE " + keyspace + "." + name + " (" + props + ");");
  }
}
