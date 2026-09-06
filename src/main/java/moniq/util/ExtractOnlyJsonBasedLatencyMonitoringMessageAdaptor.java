package moniq.util;

import moniq.exception.ImproperUsageException;

public class ExtractOnlyJsonBasedLatencyMonitoringMessageAdaptor
    extends JsonBasedLatencyMonitoringMessageAdaptor {

  @Override
  protected String getRandomPayload() {
    throw new ImproperUsageException("Extract-only adaptor cannot generate messages.");
  }
}
