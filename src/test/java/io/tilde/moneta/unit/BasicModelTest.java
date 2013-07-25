package io.tilde.moneta.unit;

import io.tilde.moneta.TestCase;
import io.tilde.moneta.annotations.Column;
import io.tilde.moneta.annotations.PrimaryKey;
import io.tilde.moneta.annotations.Table;
import io.tilde.moneta.support.Cassandra;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Objects;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class BasicModelTest extends TestCase {

  @BeforeClass
  public static void startCassandra() {
    Cassandra.start();
  }

  @Table("songs")
  public static class Song {

    @PrimaryKey("id")
    UUID key;

    @Column
    String name;

    Song(UUID key, String name) {
      this.key = key;
      this.name = name;
    }

    public Song() {
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
  public void testPersistingAndLoadingABasicModel() {
    Song song = new Song(UUID.randomUUID(), "Zomg");

    mapper().persist(song);

    assertThat(mapper().get(Song.class, song.key), equalTo(song));
  }

  @Test
  public void testZomg() {
    assertThat(true, equalTo(true));
  }
}