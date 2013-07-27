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

  /**
   * TODO:
   * - Counter
   * - Timestamp / Date
   * - Float
   * - Double
   * - Blob
   * - Decimal
   * - TimeUUID
   * - Inet
   */
  @Table("songs")
  static class Song1 {

    @PrimaryKey
    UUID id;

    @Column
    String name;

    @Column
    boolean explicit;

    @Column
    int plays;

    Song1(String name, boolean explicit, int plays) {
      this(UUID.randomUUID(), name, explicit, plays);
    }

    Song1(UUID id, String name, boolean explicit, int plays) {
      this.id = id;
      this.name = name;
      this.explicit = explicit;
      this.plays = plays;
    }

    public Song1() {
    }

    public boolean equals(Object o) {
      if (o instanceof Song1) {
        Song1 other = (Song1) o;

        return
          Objects.equals(id, other.id) &&
          Objects.equals(name, other.name) &&
          explicit == other.explicit &&
          plays == other.plays;
      }

      return false;
    }
  }

  @Test
  public void testPersistingAndLoadingABasicModel() {
    Song1 song = new Song1("Zomg", true, 3);

    mapper().persist(song);

    assertThat(mapper().get(Song1.class, song.id), equalTo(song));
  }

  @Table("songs")
  static class Song2 {

    @PrimaryKey
    UUID id;

    @Column
    long plays;

    public Song2() {
    }
  }

  @Test
  public void testLoadingVarIntAsLong() {
    Song1 song = new Song1("Zomg", true, 3);
    mapper().persist(song);

    assertThat(mapper().get(Song2.class, song.id).plays, equalTo(3L));
  }

  @Test
  public void testLoadingVarIntAsBigInteger() {
  }

  @Test
  public void testLoadingAsciiAsString() {
  }

  @Test
  public void testLoadingVarCharAsString() {
  }

}
