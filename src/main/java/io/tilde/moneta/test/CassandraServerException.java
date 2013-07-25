package io.tilde.moneta.test;

/**
 * @author Carl Lerche
 */
public class CassandraServerException extends RuntimeException {
  public CassandraServerException(String msg) {
    super(msg);
  }

  public CassandraServerException(Throwable cause) {
    super(cause);
  }
}