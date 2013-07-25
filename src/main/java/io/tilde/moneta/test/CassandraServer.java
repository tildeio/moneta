package io.tilde.moneta.test;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import org.apache.cassandra.service.CassandraDaemon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

/**
 * @author Carl Lerche
 */
public class CassandraServer {
  private static Logger LOG = LoggerFactory.getLogger(CassandraServer.class);

  public static class Builder {

    boolean clean = false;

    long connectTimeout = 50000;

    Path tmpPath;

    public Builder withClean() {
      clean = true;
      return this;
    }

    public Builder withTmpPath(Path path) {
      tmpPath = path;
      return this;
    }

    public Builder withTmpPath(String path) {
      return withTmpPath(path(path));
    }

    public Builder withConnectTimeout(long ms) {
      connectTimeout = ms;
      return this;
    }

    public CassandraServer build() {
      if (tmpPath == null)
        throw new CassandraServerConfigException("tmpPath was not specified");

      return new CassandraServer(clean, tmpPath, connectTimeout);
    }

    public CassandraServer start() {
      CassandraServer server = build();
      server.start();
      return server;
    }
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  private final boolean cleanOnStart;

  private final Path tmpPath;

  private final long connectTimeout;

  private CassandraDaemon daemon;

  private Thread thread;

  CassandraServer(boolean cleanOnStart, Path tmpPath, long connectTimeout) {
    this.cleanOnStart = cleanOnStart;
    this.tmpPath = tmpPath;
    this.connectTimeout = connectTimeout;
  }

  public void start() {
    prepare();
    configure();

    daemon = new CassandraDaemon();
    thread = new Thread() {
      public void run() {
        try {
          daemon.activate();
        }
        catch (Exception e) {
          LOG.error("error; msg={}", e.getMessage(), e);
        }
      }
    };

    // Start cassandra
    thread.start();

    // Loop until we can connect

    long start = System.nanoTime();

    try {
      while (!maybeConnect()) {
        if (TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start) > connectTimeout) {
          throw new CassandraServerException("could not start");
        }

        Thread.sleep(250);
      }
    }
    catch (InterruptedException e) {
    }

    // Find any shutdown hooks registered by cassandra
    // TODO: Proxy to them on shutdown
    try {
      Class clazz = Class.forName("java.lang.ApplicationShutdownHooks");
      Field field = clazz.getDeclaredField("hooks");
      field.setAccessible(true);
      Object hooks = field.get(null);
    }
    catch (Exception e) {
      LOG.error("error; msg={}", e.getMessage(), e);
    }
  }

  public void stop() {
    try {
      daemon.deactivate();
      thread.join(1000);
      thread.interrupt();
      thread.join(1000);
    }
    catch (InterruptedException e) {
    }
  }

  private void prepare() {
    if (cleanOnStart && Files.exists(tmpPath)) {
      deleteRecursive(tmpPath.toFile());
    }

    // Create necessary directories
    for (String path : Arrays.asList("data", "commitlog", "saved_caches")) {
      tmpPath.resolve(path(path)).toFile().mkdir();
    }

    writeConfig();
  }

  private void configure() {
    System.setProperty("cassandra.config", "file:" + cassandraYmlPath());
    System.setProperty("cassandra-foreground", "true");
  }

  private void writeConfig() {
    try {
      configPath().toFile().mkdirs();
      Files.write(cassandraYmlPath(), cassandraYml().getBytes("UTF-8"));
    } catch (IOException e) {
      LOG.error("could not write cassandra config", e);
      throw new CassandraServerException(e);
    }
  }

  private boolean maybeConnect() {
    try {
      Cluster cluster = Cluster.builder()
        .addContactPoint("127.0.0.1")
        .build();

      Metadata metadata = cluster.getMetadata();

      LOG.info("connected; cluster={}", metadata.getClusterName());

      return true;
    }
    catch (NoHostAvailableException e) {
      return false;
    }
  }

  private Path configPath() {
    return tmpPath.resolve(path("config"));
  }

  private Path cassandraYmlPath() {
    return configPath().resolve(path("cassandra.yml"));
  }

  private String cassandraYml() {
    String template = readYmlTemplate();

    if (template == null)
      return null;

    return template.replaceAll("%%tmpdir%%", tmpPath.toString());
  }

  private String readYmlTemplate() {
    try {
      URL url = CassandraServer.class.getResource("/io/tilde/moneta/test/cassandra.yml");

      if (url == null) {
        LOG.warn("cassandra.yml template does not exist");
        return null;
      }

      Path path = Paths.get(url.toURI());
      return new String(Files.readAllBytes(path), "UTF-8");
    } catch (Exception e) {
      LOG.error("could not load cassandra config template", e);
      throw new CassandraServerException(e);
    }
  }

  private static Path path(String path) {
    return Paths.get(path);
  }

  public static void deleteRecursive(File dir) {
    if (dir.isDirectory()) {
      String[] children = dir.list();

      for (String child : children)
        deleteRecursive(new File(dir, child));
    }

    // The directory is now empty so now it can be smoked
    dir.delete();
  }

}
