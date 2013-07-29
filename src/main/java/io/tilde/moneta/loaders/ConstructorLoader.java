package io.tilde.moneta.loaders;

import io.tilde.moneta.FieldMapping;
import io.tilde.moneta.MonetaLoader;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Constructor;
import java.util.Collection;

/**
 * @author Carl Lerche
 */
public abstract class ConstructorLoader<T> implements MonetaLoader<T> {

  private static MethodHandles.Lookup lookup = MethodHandles.lookup();

  private final MethodHandle constructor;

  private final Collection<FieldMapping> fields;

  public ConstructorLoader(
    Constructor<?> constructor,
    Collection<FieldMapping> fields)
    throws IllegalAccessException {

    constructor.setAccessible(true);

    // Unreflect the constructor
    this.constructor = lookup.unreflectConstructor(constructor);
    this.fields = fields;
  }

  protected Collection<FieldMapping> getFields() {
    return fields;
  }

  protected MethodHandle getConstructor() {
    return constructor;
  }

  public static <X> MonetaLoader<X> loaderFor(
    Class<X> target, Collection<FieldMapping> fields)
    throws IllegalAccessException {

    Constructor<?> candidate = null;

    for (Constructor<?> curr : target.getDeclaredConstructors()) {
      if (!isCandidate(curr, fields)) {
        continue;
      }

      candidate = bestMatch(candidate, curr);
    }

    if (candidate == null)
      return null;

    // Probably can improve this later
    if (candidate.getParameterTypes().length == 0) {
      return new DefaultConstructorLoader<>(candidate, fields);
    }
    else {
      return new ArgumentConstructorLoader<>(candidate, fields);
    }
  }

  private static boolean isCandidate(
    Constructor<?> constructor, Collection<FieldMapping> fields) {

    // Constructor params
    Class<?>[] params = constructor.getParameterTypes();

    // The default constructor is a candidate
    if (params.length == 0) {
      return true;
    }

    // If the number of params doesn't match the number of fields, then there is no match
    if (params.length != fields.size()) {
      return false;
    }

    int curr = 0;
    for (FieldMapping field : fields) {
      // for now, we only do exact matches
      if (field.getType() != params[curr]) {
        return false;
      }

      ++curr;
    }

    return true;
  }

  private static Constructor<?> bestMatch(Constructor<?> a, Constructor<?> b) {
    if (a == null)
      return b;

    if (b == null)
      return a;

    // For now, one has to have no params
    if (a.getParameterTypes().length > b.getParameterTypes().length) {
      return a;
    }
    else {
      return b;
    }
  }
}
