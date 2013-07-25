package io.tilde.moneta;

import io.tilde.moneta.annotations.Column;
import io.tilde.moneta.annotations.PrimaryKey;
import io.tilde.moneta.annotations.Table;
import io.tilde.moneta.test.CassandraServer;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.Objects;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class SimpleTest {

  @Rule
  public TemporaryFolder dir = new TemporaryFolder();

  @Table("songs")
  static class Song {

    @PrimaryKey("id")
    UUID key;

    @Column
    String name;

    Song(UUID key, String name) {
      this.key = key;
      this.name = name;
    }

    public static Song build() {
      return new Song(UUID.randomUUID(), "Zomg");
    }

    public boolean equals(Object o) {
      if (o instanceof Song) {
        Song other = (Song) o;

        return Objects.equals(key, other.key) && Objects.equals(name, other.name);
      }

      return false;
    }
  }

  @Test
  public void simpleTest() {
    CassandraServer srv = CassandraServer.newBuilder()
      .withTmpPath(dir.getRoot().toString())
      .start();

    // Setup mapper
    MonetaMapper mapper = MonetaMapper.configure()
      .withKeyspace("wtf")
      .connect();

    // Create the keyspace
    mapper.getSession().execute(
      "CREATE KEYSPACE wtf WITH replication " +
      "= {'class': 'SimpleStrategy', 'replication_factor':3};");

    // Create a table
    mapper.getSession().execute(
      "CREATE TABLE wtf.songs (id uuid PRIMARY KEY, name text);");

    Song song = Song.build();

    mapper.persist(song);

    Song s = mapper.get(Song.class, song.key);

    assertThat(s, equalTo(song));
  }
}
