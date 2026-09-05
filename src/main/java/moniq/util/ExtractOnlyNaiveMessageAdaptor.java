package moniq.util;

import moniq.exception.ImproperUsageException;

public class ExtractOnlyNaiveMessageAdaptor extends NaiveMessageAdaptor {

  public ExtractOnlyNaiveMessageAdaptor() {
    super();
  }

  /**
   * @deprecated Message size is only needed when generating messages.
   */
  @Deprecated
  public ExtractOnlyNaiveMessageAdaptor(int messageSize) {
    this();
  }

  @Override
  protected String getRandomPadding(String content) {
    throw new ImproperUsageException("Extract-only adaptor cannot generate messages.");
  }
}
