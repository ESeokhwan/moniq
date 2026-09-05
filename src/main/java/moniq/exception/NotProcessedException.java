package moniq.exception;

public class NotProcessedException extends MoniqRuntimeException {

  public NotProcessedException() {
    super("Monitor log has not been preprocessed yet.");
  }
}
