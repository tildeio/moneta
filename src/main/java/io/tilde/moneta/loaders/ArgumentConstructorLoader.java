package io.tilde.moneta.loaders;

import com.datastax.driver.core.Row;
import io.tilde.moneta.FieldMapping;
import io.tilde.moneta.MonetaLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Loads objects by passing the property values as arguments to the constructor
 *
 * @author Carl Lerche
 */
public class ArgumentConstructorLoader<T> extends ConstructorLoader<T> {
  private static Logger LOG =
    LoggerFactory.getLogger(ArgumentConstructorLoader.class);

  public ArgumentConstructorLoader(
    Constructor<?> constructor,
    Collection<FieldMapping> fields)
    throws IllegalAccessException {
    super(constructor, fields);
  }

  public T load(Row row) {
    List<Object> arguments = new ArrayList<>(getFields().size());

    for (FieldMapping field : getFields()) {
      arguments.add(field.cast(row));
    }

    return build(arguments);
  }

  @SuppressWarnings("unchecked")
  private T build(List<?> arguments) {
    try {
      return (T) getConstructor().invokeWithArguments(arguments);
    }
    catch (Throwable t) {
      LOG.warn("could not create instance; ex={}", t);
      return null;
    }
  }

}
