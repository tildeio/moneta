package io.tilde.moneta;

import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.tilde.moneta.annotations.Column;
import io.tilde.moneta.annotations.PrimaryKey;
import io.tilde.moneta.annotations.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;

/**
 * @author Carl Lerche
 */
class Mapping {
  private static Logger LOG = LoggerFactory.getLogger(Mapping.class);

  private final Class<?> target;

  private final String keyspace;

  private final String table;

  private final Map<String, FieldMapping> fields;

  Mapping(Class<?> target, String keyspace) throws IllegalAccessException {
    this(target, keyspace, null);
  }

  Mapping(Class<?> target, String keyspace, String table) throws IllegalAccessException {
    this.target = target;
    this.keyspace = keyspace;
    this.table = table != null ? table : tableFor(target);
    this.fields = fieldMappingsFor(target);

    if (fields.isEmpty())
      throw new IllegalArgumentException("target class has no defined columns");
  }

  private static String tableFor(Class<?> target) {
    Table table = target.getAnnotation(Table.class);

    if (table == null)
      throw new RuntimeException("Table annotation missing for " + target.getName());

    return table.value();
  }

  <T> ListenableFuture<T> get(Session session, final Class<T> klass, Object key) {
    Query query = QueryBuilder.select()
      .from(keyspace, table)
      .where(eq("id", key));

    LOG.debug("get; query={}", query);

    return Futures.transform(
      session.executeAsync(query),
      new Function<ResultSet, T>() {
        public T apply(ResultSet res) {
          return load(klass, res.one());
        }
      });
  }

  <T> T load(Class<T> klass, Row row) {
    T inst = build(klass);

    if (inst == null)
      return null;

    for (FieldMapping mapping : fields.values()) {
      mapping.set(inst, row);
    }

    return inst;
  }

  static <T> T build(Class<T> klass) {
    try {
      return klass.newInstance();
    }
    catch (InstantiationException e) {
      LOG.warn("could not create instance; klass={}", klass);
      return null;
    }
    catch (IllegalAccessException e) {
      LOG.warn("could not create instance; klass={}; msg={}", klass, e.getMessage(), e);
      return null;
    }
  }

  <T> ListenableFuture<T> persist(Session session, T obj) {
    Insert query = QueryBuilder.insertInto(keyspace, table);

    for (Map.Entry<String, FieldMapping> field : fields.entrySet()) {
      query.value(field.getKey(), field.getValue().get(obj));
    }

    LOG.debug("persisting; query={}", query);

    return Futures.transform(session.executeAsync(query), Functions.constant(obj));
  }

  private static Map<String, FieldMapping> fieldMappingsFor(Class<?> target)
    throws IllegalAccessException {

    Map<String, FieldMapping> ret = new HashMap<>();

    for (Field field : target.getDeclaredFields()) {
      PrimaryKey pk = field.getAnnotation(PrimaryKey.class);
      Column col = field.getAnnotation(Column.class);

      if (pk == null && col == null) {
        continue;
      }

      FieldMapping mapping;

      if (pk != null) {
        mapping = FieldMapping.build(target, pk.value(), field);
      }
      else {
        mapping = FieldMapping.build(target, col.value(), field);
      }

      ret.put(mapping.getName(), mapping);
    }

    return ret;
  }
}