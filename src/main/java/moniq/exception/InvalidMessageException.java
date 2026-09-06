package moniq.exception;

public class InvalidMessageException extends MoniqRuntimeException {

  public InvalidMessageException(String message) {
    super(message);
  }

  public InvalidMessageException(String message, Throwable cause) {
    super(message, cause);
  }
}
