package io.tilde.moneta.support;

import io.tilde.moneta.test.CassandraServer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Ensures that a cassandra server is running
 */
public class Cassandra {

  private static CassandraServer daemon = null;

  public static void start() {
    if (daemon != null) {
      return;
    }

    try {
      final Path tmpPath = Files.createTempDirectory("moneta-tests");
      daemon = CassandraServer.newBuilder().withTmpPath(tmpPath).start();

      Runtime.getRuntime().addShutdownHook(new Thread() {
        public void run() {
          daemon.stop();
          CassandraServer.deleteRecursive(tmpPath.toFile());
        }
      });
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

}
