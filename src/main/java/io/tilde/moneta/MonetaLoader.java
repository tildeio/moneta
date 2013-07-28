package io.tilde.moneta;

import com.datastax.driver.core.Row;

/**
 * Initializes objects of type T based on the given CQL row.
 *
 * @param <T> The type that is loaded
 */
public interface MonetaLoader<T> {

  T load(Row row);

}
