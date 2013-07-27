package io.tilde.moneta;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.exceptions.InvalidTypeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.UUID;

abstract class FieldMapping {
  private static Logger LOG = LoggerFactory.getLogger(FieldMapping.class);

  private static MethodHandles.Lookup lookup = MethodHandles.lookup();

  static FieldMapping build(Class<?> target, String name, Field field)
    throws IllegalAccessException {

    // We need to bypass checks
    field.setAccessible(true);

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
    else if (boolean.class.isAssignableFrom(type)) {
      return new BooleanFieldMapping(name, getter, setter);
    }
    else if (int.class.isAssignableFrom(type)) {
      return new IntegerFieldMapping(name, getter, setter);
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
    }
    catch (Throwable t) {
      throw new RuntimeException(t);
    }
  }

  void set(Object obj, Row row) {
    DataType type = row.getColumnDefinitions().getType(getName());

    switch (type.getName()) {
      case BOOLEAN:
        set(obj, row.getBool(getName()));
        break;

      case INT:
        set(obj, row.getInt(getName()));
        break;

      case BIGINT:
      case COUNTER:
        set(obj, row.getLong(getName()));
        break;

      case TIMESTAMP:
        set(obj, row.getDate(getName()));
        break;

      case FLOAT:
        set(obj, row.getFloat(getName()));
        break;

      case DOUBLE:
        set(obj, row.getDouble(getName()));
        break;

      case BLOB:
        set(obj, row.getBytes(getName()));
        break;

      case ASCII:
      case TEXT:
      case VARCHAR:
        set(obj, row.getString(getName()));
        break;

      case DECIMAL:
        set(obj, row.getDecimal(getName()));
        break;

      case UUID:
      case TIMEUUID:
        set(obj, row.getUUID(getName()));
        break;

      case INET:
        set(obj, row.getInet(getName()));
        break;

      case LIST:
      case SET:
      case MAP:
        throw new InvalidTypeException("collection support not implemented");

      case VARINT:
        set(obj, row.getVarint(getName()));
        break;

      default:
        throw new InvalidTypeException("unknown type");
    }
  }

  protected void set(Object obj, boolean val) {
    set(obj, (Object) val);
  }

  protected void set(Object obj, int val) {
    set(obj, (Object) val);
  }

  protected void set(Object obj, long val) {
    set(obj, (Object) val);
  }

  protected void set(Object obj, Date val) {
    set(obj, (Object) val);
  }

  protected void set(Object obj, double val) {
    set(obj, (Object) val);
  }

  protected void set(Object obj, float val) {
    set(obj, (Object) val);
  }

  protected void set(Object obj, ByteBuffer val) {
    set(obj, (Object) val);
  }

  protected void set(Object obj, String val) {
    set(obj, (Object) val);
  }

  protected void set(Object obj, BigDecimal val) {
    set(obj, (Object) val);
  }

  protected void set(Object obj, UUID val) {
    set(obj, (Object) val);
  }

  protected void set(Object obj, InetAddress val) {
    set(obj, (Object) val);
  }

  protected void set(Object obj, BigInteger val) {
    set(obj, (Object) val);
  }

  protected void set(Object obj, Object val) {
    throw new InvalidTypeException(
      "cannot convert `" + val.getClass() + "` to `" + type() + "`");
  }

  protected Class<?> type() {
    return this.getClass();
  }

  /**
   * Performs the set on the object with the given value.
   *
   * @param obj the target object of the set
   * @param val the value to set
   */
  protected void setRaw(Object obj, Object val) {
    LOG.trace("setRaw; obj={}; name={}; val={}", obj, name, val);
    try {
      setter.invoke(obj, val);
    }
    catch (Throwable t) {
      throw new RuntimeException(t);
    }
  }

  static class UUIDFieldMapping extends FieldMapping {
    UUIDFieldMapping(String name, MethodHandle getter, MethodHandle setter) {
      super(name, getter, setter);
    }

    protected void set(Object obj, UUID val) {
      setRaw(obj, val);
    }
  }

  static class StringFieldMapping extends FieldMapping {
    StringFieldMapping(String name, MethodHandle getter, MethodHandle setter) {
      super(name, getter, setter);
    }

    protected void set(Object obj, String val) {
      setRaw(obj, val);
    }
  }

  static class BooleanFieldMapping extends FieldMapping {
    BooleanFieldMapping(String name, MethodHandle getter, MethodHandle setter) {
      super(name, getter, setter);
    }

    protected void set(Object obj, boolean val) {
      setRaw(obj, val);
    }
  }

  static class IntegerFieldMapping extends FieldMapping {
    IntegerFieldMapping(String name, MethodHandle getter, MethodHandle setter) {
      super(name, getter, setter);
    }

    private static final BigInteger MAXINT = BigInteger.valueOf(Integer.MAX_VALUE);
    private static final BigInteger MININT = BigInteger.valueOf(Integer.MIN_VALUE);

    protected void set(Object obj, BigInteger val) {
      if (!MININT.min(val).equals(MININT) || !MAXINT.max(val).equals(MAXINT)) {
        throw new InvalidTypeException("varint out of int range");
      }

      setRaw(obj, val.intValue());
    }

    protected void set(Object obj, int val) {
      setRaw(obj, val);
    }

    protected void set(Object obj, long val) {
      if (val < Integer.MIN_VALUE || val > Integer.MAX_VALUE) {
        throw new InvalidTypeException("bigint out of int range");
      }

      setRaw(obj, (int) val);
    }
  }
}
