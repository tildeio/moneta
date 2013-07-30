package io.tilde.moneta;

import com.datastax.driver.core.Session;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * Configures and builds an instance of MonetaMapper.
 *
 * @author Carl Lerche
 */
public class MonetaMapper {
  private static Logger LOG = LoggerFactory.getLogger(MonetaMapper.class);

  private final Session session;

  private final String keyspace;

  private Map<Class<?>, Mapping> mappings = ImmutableMap.of();

  MonetaMapper(Session session, String keyspace) {
    this.session = session;
    this.keyspace = keyspace;
  }

  public static MonetaConfig configure() {
    return new MonetaConfig();
  }

  public void close() {
    // session.shutdown();
  }

  public Session getSession() {
    return session;
  }

  public <T> T get(Class<T> klass, Object key) {
    try {
      return getAsync(klass, key).get();
    }
    catch (InterruptedException e) {
      return null;
    }
    catch (ExecutionException e) {
      if (e.getCause() instanceof RuntimeException) {
        throw (RuntimeException) e.getCause();
      }
      else {
        throw new RuntimeException(e.getCause());
      }
    }
  }

  public <T> T get(Class<T> klass, Object part, Object... parts) {
    return get(klass, listifyVarargs(part, parts));
  }

  public <T> T get(Class<T> klass, List<? extends Object> key) {
    return get(klass, new CompositeKey(key));
  }

  public <T> ListenableFuture<T> getAsync(Class<T> klass, Object key) {
    return mappingFor(klass).get(session, key);
  }

  public <T> ListenableFuture<T> getAsync(Class<T> klass, Object part, Object... parts) {
    return getAsync(klass, listifyVarargs(part, parts));
  }

  public <T> ListenableFuture<T> getAsync(Class<T> klass, List<? extends Object> key) {
    return getAsync(klass, new CompositeKey(key));
  }

  public <T> Collection<T> getAll(Class<T> klass, Iterable<?> keys) {
    return null;
  }

  public <T> ListenableFuture<Collection<T>> getAllAsync(
    Class<T> klass, Iterable<?> keys) {
    return null;
  }

  public <T> T persist(T obj) {
    try {
      return persistAsync(obj).get();
    }
    catch (InterruptedException e) {
      return null;
    }
    catch (ExecutionException e) {
      return null;
    }
  }

  public <T> ListenableFuture<T> persistAsync(T obj) {
    return mappingFor(obj).persist(session, obj);
  }

  public <T> void delete(Class<T> klass, Object key) {
  }

  public <T> void invalidate(Class<T> klass, Object key) {
  }

  public <T> void invalidateAll(Class<T> klass, Iterable<?> keys) {
  }

  private Mapping mappingFor(Class<?> klass) {
    Mapping ret = mappings.get(klass);

    if (ret == null) {
      synchronized (this) {
        // Try again with lock
        ret = mappings.get(klass);

        if (ret == null) {
          try {
            ret = new Mapping(klass, keyspace);
            mappings = ImmutableMap.<Class<?>, Mapping>builder()
              .putAll(mappings)
              .put(klass, ret)
              .build();
          }
          catch (IllegalAccessException e) {
            LOG.warn("could not map {}; msg={}", klass, e.getMessage(), e);
            return null;
          }
        }
      }
    }

    return ret;
  }

  private <T> Mapping mappingFor(T obj) {
    return mappingFor(obj.getClass());
  }

  private List<Object> listifyVarargs(Object first, Object[] rest) {
    List<Object> ret = new ArrayList<>(rest.length + 1);

    ret.add(first);

    for (int i = 0; i < rest.length; ++i) {
      ret.add(rest[i]);
    }

    return ret;
  }
}