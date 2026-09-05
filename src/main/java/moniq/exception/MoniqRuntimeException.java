package moniq.exception;

public class MoniqRuntimeException extends RuntimeException {

  public MoniqRuntimeException(String message) {
    super(message);
  }

  public MoniqRuntimeException(String message, Throwable cause) {
    super(message, cause);
  }
}
