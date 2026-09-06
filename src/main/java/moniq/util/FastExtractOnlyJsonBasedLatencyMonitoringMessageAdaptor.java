package moniq.util;

import moniq.exception.ImproperUsageException;

/** Parses fast JSON latency messages but deliberately does not support generation. */
public class FastExtractOnlyJsonBasedLatencyMonitoringMessageAdaptor
    extends FastJsonBasedLatencyMonitoringMessageAdaptor {

  @Override
  protected String getRandomPayload() {
    throw new ImproperUsageException("Extract-only adaptor cannot generate messages.");
  }
}
