package io.tilde.moneta;

import com.datastax.driver.core.querybuilder.Clause;

import java.util.ArrayList;
import java.util.List;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;

/**
 * @author Carl Lerche
 */
public class CompositeKeyMapping extends KeyMapping {

  final List<FieldMapping> primaryFields;

  public CompositeKeyMapping(List<FieldMapping> primaryFields) {
    this.primaryFields = primaryFields;
  }

  public List<Clause> predicateForGet(Object key) {
    if (key instanceof CompositeKey) {
      return predicateForGet((CompositeKey) key);
    }

    throw new IllegalArgumentException("key is not composite");
  }

  public List<Clause> predicateForGet(CompositeKey key) {
    if (key.size() != primaryFields.size()) {
      throw new IllegalArgumentException(
        "key arity (" + key.size() + ") does not match model (" + primaryFields
          .size() + ")");
    }

    List<Clause> ret = new ArrayList<>(primaryFields.size());

    for (int i = 0; i < primaryFields.size(); ++i) {
      ret.add(eq(primaryFields.get(i).getName(), key.get(i)));
    }

    return ret;
  }

}
