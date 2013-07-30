package io.tilde.moneta;

import java.util.Collections;
import java.util.List;

/**
 * @author Carl Lerche
 */
public class CompositeKey {

  final List<? extends Object> components;

  final int hashCode;

  public CompositeKey(List<? extends Object> components) {
    this.components = Collections.unmodifiableList(components);
    this.hashCode = components.hashCode();
  }

  public Object get(int idx) {
    return components.get(idx);
  }

  public int size() {
    return components.size();
  }

  public int hashCode() {
    return hashCode;
  }

  public boolean equals(Object o) {
    if (o instanceof CompositeKey) {
      return components.equals(((CompositeKey) o).components);
    }

    return false;
  }
}
