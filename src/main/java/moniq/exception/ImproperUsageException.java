package moniq.exception;

public class ImproperUsageException extends MoniqRuntimeException {

  public ImproperUsageException() {
    super("Improper usage.");
  }

  public ImproperUsageException(String message) {
    super(message);
  }
}
