package moniq.util;

import moniq.exception.ImproperUsageException;

public class FastExtractOnlyJsonBasedLatencyMonitoringMessageAdaptor
    extends FastJsonBasedLatencyMonitoringMessageAdaptor {

  @Override
  protected String getRandomPayload() {
    throw new ImproperUsageException("Extract-only adaptor cannot generate messages.");
  }
}
