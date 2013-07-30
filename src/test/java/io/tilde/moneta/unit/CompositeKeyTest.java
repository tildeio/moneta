package io.tilde.moneta.unit;

import io.tilde.moneta.TestCase;
import io.tilde.moneta.annotations.PrimaryKey;
import io.tilde.moneta.annotations.Table;
import org.junit.Test;

import java.util.Objects;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class CompositeKeyTest extends TestCase {

  @Table("playlists")
  static class Playlist {

    @PrimaryKey
    final UUID id;

    @PrimaryKey
    final String title;

    @PrimaryKey
    final String album;

    Playlist(UUID id, String title, String album) {
      this.id = id;
      this.title = title;
      this.album = album;
    }

    public boolean equals(Object other) {
      if (other instanceof Playlist) {
        Playlist o = (Playlist) other;

        return Objects.equals(id, o.id) &&
          Objects.equals(title, o.title) &&
          Objects.equals(album, o.album);
      }

      return false;
    }

    public String toString() {
      return String.format("Playlist#{%s, %s, %s}", id, title, album);
    }
  }

  @Test
  public void testGetCompositeKeyModel() {
    UUID uuid = UUID.randomUUID();
    Playlist p1 = new Playlist(uuid, "Foo", "Bar");
    Playlist p2 = new Playlist(uuid, "Foo", "Baz");

    mapper().persist(p1);
    mapper().persist(p2);

    Playlist p3 = mapper().get(Playlist.class, p1.id, "Foo", "Bar");
    assertThat(p3, equalTo(p1));

    Playlist p4 = mapper().get(Playlist.class, p2.id, "Foo", "Baz");
    assertThat(p4, equalTo(p2));

  }
}
