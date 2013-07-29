package io.tilde.moneta.unit;

import io.tilde.moneta.TestCase;
import io.tilde.moneta.annotations.Column;
import io.tilde.moneta.annotations.PrimaryKey;
import io.tilde.moneta.annotations.Table;
import org.junit.Test;

import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class ConstructingTest extends TestCase {

  @Table("songs")
  static class Song1 {

    @PrimaryKey
    final UUID id;

    @Column
    final String name;

    Song1(String name) {
      this(UUID.randomUUID(), name);
    }

    Song1(UUID id, String name) {
      this.id = id;
      this.name = name;
    }
  }

  @Test
  public void testLoadingModelWithCustomConstructor() {
    Song1 songA = new Song1("My Song");
    mapper().persist(songA);

    Song1 songB = mapper().get(Song1.class, songA.id);

    assertThat(songB.id, equalTo(songA.id));
    assertThat(songB.name, equalTo(songA.name));
  }

  @Test
  public void testLoadingModelWithCustomLoader() {
    // Specify a loading class
  }
}
