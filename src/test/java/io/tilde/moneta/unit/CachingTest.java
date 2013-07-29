package io.tilde.moneta.unit;

import io.tilde.moneta.TestCase;
import io.tilde.moneta.annotations.Cached;
import io.tilde.moneta.annotations.Column;
import io.tilde.moneta.annotations.PrimaryKey;
import io.tilde.moneta.annotations.Table;
import org.junit.Test;

import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class CachingTest extends TestCase {

  @Table("songs")
  @Cached
  static class CachedSong {

    @PrimaryKey
    UUID id;

    @Column
    String name;

    CachedSong(UUID id, String name) {
      this.id = id;
      this.name = name;
    }
  }

  @Test
  public void testLoadingCachableModelCachesResult() {
    CachedSong song = new CachedSong(UUID.randomUUID(), "Cached");
    mapper().persist(song);

    // Fill cache
    song = mapper().get(CachedSong.class, song.id);

    // Delete the song
    session().execute("DELETE FROM moneta.songs WHERE id = " + song.id);

    // It should still be there
    CachedSong cached = mapper().get(CachedSong.class, song.id);

    assertThat(cached.id, equalTo(song.id));
  }

}
