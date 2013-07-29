package io.tilde.moneta.loaders;

import com.datastax.driver.core.Row;
import io.tilde.moneta.FieldMapping;
import io.tilde.moneta.MonetaLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Constructor;
import java.util.Collection;

/**
 * Loads objects by constructing the object with the default constructor,
 * then setting the fields directly.
 *
 * @author Carl Lerche
 */
public class DefaultConstructorLoader<T> extends ConstructorLoader<T> {

  private static Logger LOG =
    LoggerFactory.getLogger(DefaultConstructorLoader.class);

  public DefaultConstructorLoader(
    Constructor<?> constructor,
    Collection<FieldMapping> fields)
    throws IllegalAccessException {
    super(constructor, fields);
  }

  public T load(Row row) {
    T inst = build();

    if (inst == null)
      return null;

    for (FieldMapping field : getFields()) {
      field.set(inst, field.cast(row));
    }

    return inst;
  }

  @SuppressWarnings("unchecked")
  private T build() {
    try {
      return (T) getConstructor().invoke();
    }
    catch (Throwable t) {
      LOG.warn("could not create instance; ex={}", t);
      return null;
    }
  }

}
