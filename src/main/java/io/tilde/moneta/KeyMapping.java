package io.tilde.moneta;

import com.datastax.driver.core.querybuilder.Clause;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Carl Lerche
 */
public abstract class KeyMapping {

  public static KeyMapping mappingFor(List<FieldMapping> fields) {
    List<FieldMapping> primary = new ArrayList<>();

    for (FieldMapping field : fields) {
      if (field.isPrimary()) {
        primary.add(field);
      }
    }

    if (primary.size() > 1) {
      return new CompositeKeyMapping(primary);
    }
    else if (primary.size() == 1) {
      return new SingleKeyMapping(primary.get(0));
    }
    else {
      throw new IllegalArgumentException("no primary fields specified");
    }
  }

  public abstract List<Clause> predicateForGet(Object key);

}
