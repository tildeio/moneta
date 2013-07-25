package io.tilde.moneta;

import com.datastax.driver.core.Row;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Field;
import java.util.UUID;

abstract class FieldMapping {

  private static MethodHandles.Lookup lookup = MethodHandles.lookup();

  static FieldMapping build(Class<?> target, String name, Field field)
    throws IllegalAccessException {

    Class<?> type = field.getType();
    name = name.equals("-") ? field.getName() : name;
    MethodHandle getter = lookup.unreflectGetter(field);
    MethodHandle setter = lookup.unreflectSetter(field);

    if (UUID.class.isAssignableFrom(type)) {
      return new UUIDFieldMapping(name, getter, setter);
    }
    else if (String.class.isAssignableFrom(type)) {
      return new StringFieldMapping(name, getter, setter);
    }
    else {
      throw new RuntimeException("can't handle fields of type `" + type + "`");
    }
  }

  private final String name;

  private final MethodHandle getter;

  private final MethodHandle setter;

  FieldMapping(String name, MethodHandle getter, MethodHandle setter) {
    this.name = name;
    this.getter = getter;
    this.setter = setter;
  }

  String getName() {
    return name;
  }

  Object get(Object obj) {
    try {
      return getter.invoke(obj);
    } catch (Throwable t) {
      throw new RuntimeException(t);
    }
  }

  void set(Object obj, Object val) {
    try {
      setter.invoke(obj, val);
    } catch (Throwable t) {
      throw new RuntimeException(t);
    }
  }

  abstract void setFrom(Object obj, Row row);

  static class UUIDFieldMapping extends FieldMapping {
    UUIDFieldMapping(String name, MethodHandle getter, MethodHandle setter) {
      super(name, getter, setter);
    }

    void setFrom(Object obj, Row row) {
      set(obj, row.getUUID(getName()));
    }
  }

  static class StringFieldMapping extends FieldMapping {
    StringFieldMapping(String name, MethodHandle getter, MethodHandle setter) {
      super(name, getter, setter);
    }

    void setFrom(Object obj, Row row) {
      set(obj, row.getString(getName()));
    }
  }
}
