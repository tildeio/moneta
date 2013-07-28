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
public class ArgumentConstructorLoader<T> implements MonetaLoader<T> {
  private static Logger LOG =
    LoggerFactory.getLogger(ArgumentConstructorLoader.class);

  private static MethodHandles.Lookup lookup = MethodHandles.lookup();

  private final MethodHandle constructor;

  private final Collection<FieldMapping> fields;

  public ArgumentConstructorLoader(
    Constructor<?> constructor,
    Collection<FieldMapping> fields)
    throws IllegalAccessException {

    // Simple check
    if (constructor.getParameterTypes().length != fields.size())
      throw new IllegalArgumentException("invalid constructor");

    constructor.setAccessible(true  );

    // Unreflect the constructor
    this.constructor = lookup.unreflectConstructor(constructor);
    this.fields = fields;
  }

  public T load(Row row) {
    List<Object> arguments = new ArrayList<>(fields.size());

    for (FieldMapping field : fields) {
      arguments.add(field.cast(row));
    }

    return build(arguments);
  }

  @SuppressWarnings("unchecked")
  private T build(List<?> arguments) {
    try {
      return (T) constructor.invokeWithArguments(arguments);
    }
    catch (Throwable t) {
      LOG.warn("could not create instance; ex={}", t);
      return null;
    }
  }

}
