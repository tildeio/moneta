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

public abstract class FieldMapping {
  private static Logger LOG = LoggerFactory.getLogger(FieldMapping.class);

  private static MethodHandles.Lookup lookup = MethodHandles.lookup();

  static class Params {
    final String name;

    final Class<?> type;

    final MethodHandle getter;

    final MethodHandle setter;

    Params(Class<?> type, String name, MethodHandle g, MethodHandle s) {
      this.type = type;
      this.name = name;
      this.getter = g;
      this.setter = s;
    }
  }

  static FieldMapping build(String name, Field field)
    throws IllegalAccessException {

    // We need to bypass checks
    field.setAccessible(true);

    Class<?> type = field.getType();
    Params params = new Params(type,
      name.equals("-") ? field.getName() : name,
      lookup.unreflectGetter(field),
      lookup.unreflectSetter(field));

    if (UUID.class.isAssignableFrom(type)) {
      return new UUIDFieldMapping(params);
    }
    else if (String.class.isAssignableFrom(type)) {
      return new StringFieldMapping(params);
    }
    else if (boolean.class.isAssignableFrom(type)) {
      return new BooleanFieldMapping(params);
    }
    else if (int.class.isAssignableFrom(type)) {
      return new IntegerFieldMapping(params);
    }
    else if (long.class.isAssignableFrom(type)) {
      return new LongFieldMapping(params);
    }
    else {
      throw new RuntimeException("can't handle fields of type `" + type + "`");
    }
  }

  private final Class<?> type;

  private final String name;

  private final MethodHandle getter;

  private final MethodHandle setter;

  FieldMapping(Params params) {
    this.type = params.type;
    this.name = params.name;
    this.getter = params.getter;
    this.setter = params.setter;
  }

  public Class<?> getType() {
    return type;
  }

  public String getName() {
    return name;
  }

  public Object get(Object obj) {
    try {
      return getter.invoke(obj);
    }
    catch (Throwable t) {
      throw new RuntimeException(t);
    }
  }

  public Object cast(Row row) {
    DataType type = row.getColumnDefinitions().getType(getName());

    switch (type.getName()) {
      case BOOLEAN:
        return cast(row.getBool(getName()));

      case INT:
        return cast(row.getInt(getName()));

      case BIGINT:
      case COUNTER:
        return cast(row.getLong(getName()));

      case TIMESTAMP:
        return cast(row.getDate(getName()));

      case FLOAT:
        return cast(row.getFloat(getName()));

      case DOUBLE:
        return cast(row.getDouble(getName()));

      case BLOB:
        return cast(row.getBytes(getName()));

      case ASCII:
      case TEXT:
      case VARCHAR:
        return cast(row.getString(getName()));

      case DECIMAL:
        return cast(row.getDecimal(getName()));

      case UUID:
      case TIMEUUID:
        return cast(row.getUUID(getName()));

      case INET:
        return cast(row.getInet(getName()));

      case LIST:
      case SET:
      case MAP:
        throw new InvalidTypeException("collection support not implemented");

      case VARINT:
        return cast(row.getVarint(getName()));

      default:
        throw new InvalidTypeException("unknown type");
    }
  }

  protected Object cast(boolean val) {
    throw castEx(val);
  }

  protected Object cast(int val) {
    throw castEx(val);
  }

  protected Object cast(long val) {
    throw castEx(val);
  }

  protected Object cast(Date val) {
    throw castEx(val);
  }

  protected Object cast(double val) {
    throw castEx(val);
  }

  protected Object cast(float val) {
    throw castEx(val);
  }

  protected Object cast(ByteBuffer val) {
    throw castEx(val);
  }

  protected Object cast(String val) {
    throw castEx(val);
  }

  protected Object cast(BigDecimal val) {
    throw castEx(val);
  }

  protected Object cast(UUID val) {
    throw castEx(val);
  }

  protected Object cast(InetAddress val) {
    throw castEx(val);
  }

  protected Object cast(BigInteger val) {
    throw castEx(val);
  }

  private RuntimeException castEx(Object val) {
    return new InvalidTypeException(
      "cannot convert `" + val.getClass() + "` to `" + getType() + "`");
  }

  /**
   * Performs the set on the object with the given value.
   *
   * @param obj the target object of the set
   * @param val the value to set
   */
  public void set(Object obj, Object val) {
    LOG.trace("setRaw; obj={}; name={}; val={}", obj, name, val);
    try {
      setter.invoke(obj, val);
    }
    catch (Throwable t) {
      throw new RuntimeException(t);
    }
  }

  static class UUIDFieldMapping extends FieldMapping {
    UUIDFieldMapping(Params params) {
      super(params);
    }

    protected Object cast(UUID val) {
      return val;
    }
  }

  static class StringFieldMapping extends FieldMapping {
    StringFieldMapping(Params params) {
      super(params);
    }

    protected Object cast(String val) {
      return val;
    }
  }

  static class BooleanFieldMapping extends FieldMapping {
    BooleanFieldMapping(Params params) {
      super(params);
    }

    protected Object cast(boolean val) {
      return val;
    }
  }

  static class IntegerFieldMapping extends FieldMapping {
    IntegerFieldMapping(Params params) {
      super(params);
    }

    private static final BigInteger MAXINT = BigInteger.valueOf(Integer.MAX_VALUE);
    private static final BigInteger MININT = BigInteger.valueOf(Integer.MIN_VALUE);

    protected Object cast(BigInteger val) {
      if (!MININT.min(val).equals(MININT) || !MAXINT.max(val).equals(MAXINT)) {
        throw new InvalidTypeException("varint out of int range");
      }

      return val.intValue();
    }

    protected Object cast(int val) {
      return val;
    }

    protected Object cast(long val) {
      if (val < Integer.MIN_VALUE || val > Integer.MAX_VALUE) {
        throw new InvalidTypeException("bigint out of int range");
      }

      return val;
    }
  }

  static class LongFieldMapping extends FieldMapping {
    LongFieldMapping(Params params) {
      super(params);
    }

    private static final BigInteger MAXLONG = BigInteger.valueOf(Long.MAX_VALUE);
    private static final BigInteger MINLONG = BigInteger.valueOf(Long.MIN_VALUE);

    protected Object cast(BigInteger val) {
      if (!MINLONG.min(val).equals(MINLONG) || !MAXLONG.max(val).equals(MAXLONG)) {
        throw new InvalidTypeException("varint out of int range");
      }

      return val.longValue();
    }

    protected Object cast(int val) {
      return (long) val;
    }

    protected Object cast(long val) {
      return val;
    }
  }
}
