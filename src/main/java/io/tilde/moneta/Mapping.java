package io.tilde.moneta;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.tilde.moneta.annotations.Cached;
import io.tilde.moneta.annotations.Column;
import io.tilde.moneta.annotations.PrimaryKey;
import io.tilde.moneta.annotations.Table;
import io.tilde.moneta.loaders.ConstructorLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 *
 * @author Carl Lerche
 */
class Mapping<T> {
  private static Logger LOG = LoggerFactory.getLogger(Mapping.class);

  // Used to lookup the classes constructor
  private static MethodHandles.Lookup lookup = MethodHandles.lookup();

  private final Class<T> target;

  private final String keyspace;

  private final String table;

  private final MonetaLoader<T> loader;

  private final KeyMapping primaryKey;

  private final List<FieldMapping> fields;

  private final Cache<Object, T> cache;

  Mapping(Class<T> target, String keyspace)
    throws IllegalAccessException {
    this(target, keyspace, null);
  }

  Mapping(Class<T> target, String keyspace, String table)
    throws IllegalAccessException {

    this.target = target;
    this.keyspace = keyspace;
    this.table = table != null ? table : tableFor(target);
    this.fields = fieldMappingsFor(target);

    if (fields.isEmpty())
      throw new IllegalArgumentException("target class has no defined columns");

    this.primaryKey = KeyMapping.mappingFor(fields);
    this.loader = ConstructorLoader.loaderFor(target, fields);
    this.cache = cacheFor(target);
  }

  private static String tableFor(Class<?> target) {
    Table table = target.getAnnotation(Table.class);

    if (table == null)
      throw new RuntimeException("Table annotation missing for " + target.getName());

    return table.value();
  }

  public ListenableFuture<T> get(Session session, final Object key) {
    // Check the cache first
    if (cache != null) {
      T ret = cache.getIfPresent(key);

      if (ret != null) {
        return Futures.immediateFuture(ret);
      }
    }

    Select query = QueryBuilder.select()
      .from(keyspace, table);

    for (Clause clause : primaryKey.predicateForGet(key)) {
      query.where(clause);
    }

    LOG.debug("get; query={}", query);

    return Futures.transform(
      session.executeAsync(query),
      new Function<ResultSet, T>() {
        public T apply(ResultSet res) {
          return load(key, res.one());
        }
      });
  }

  T load(Object key, Row row) {
    if (cache == null)
      return loader.load(row);

    T ret = cache.getIfPresent(key);

    if (ret != null)
      return ret;

    ret = loader.load(row);
    cache.put(key, ret);
    return ret;
  }

  ListenableFuture<T> persist(Session session, T obj) {
    // TODO: cache on persist
    Insert query = QueryBuilder.insertInto(keyspace, table);

    for (FieldMapping field : fields) {
      query.value(field.getName(), field.get(obj));
    }

    LOG.debug("persisting; query={}", query);

    return Futures.transform(session.executeAsync(query), Functions.constant(obj));
  }

  private static List<FieldMapping> fieldMappingsFor(Class<?> target)
    throws IllegalAccessException {

    List<FieldMapping> ret = new ArrayList<>();

    for (Field field : target.getDeclaredFields()) {
      PrimaryKey pk = field.getAnnotation(PrimaryKey.class);
      Column col = field.getAnnotation(Column.class);

      if (pk == null && col == null) {
        continue;
      }

      FieldMapping mapping;

      if (pk != null) {
        mapping = FieldMapping.build(pk.value(), true, field);
      }
      else {
        mapping = FieldMapping.build(col.value(), false, field);
      }

      ret.add(mapping);
    }

    return ret;
  }

  private static <X> Cache<Object, X> cacheFor(Class<X> target) {
    Cached cached = target.getAnnotation(Cached.class);

    if (cached == null)
      return null;

    return CacheBuilder.newBuilder()
      .maximumSize(1000)
      .expireAfterAccess(10, TimeUnit.MINUTES)
      .build();
  }
}