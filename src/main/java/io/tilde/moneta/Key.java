package io.tilde.moneta;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;

/**
 * Helper class for creating composite keys
 *
 * @author Carl Lerche
 */
public class Key {

  final List<Object> components;

  public Key(Iterable<Object> components) {
    components = new ImmutableList.Builder().addAll(components).build();
  }
}
