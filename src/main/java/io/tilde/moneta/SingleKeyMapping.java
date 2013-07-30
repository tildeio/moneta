package io.tilde.moneta;

import com.datastax.driver.core.querybuilder.Clause;

import java.util.Arrays;
import java.util.List;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;

public class SingleKeyMapping extends KeyMapping {

  private final FieldMapping field;

  public SingleKeyMapping(FieldMapping field) {
    this.field = field;
  }

  public List<Clause> predicateForGet(Object key) {
    return Arrays.asList(eq(field.getName(), key));
  }

}
