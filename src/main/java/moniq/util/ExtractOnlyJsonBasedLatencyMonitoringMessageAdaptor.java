package moniq.util;

import moniq.exception.ImproperUsageException;

/** Parses JSON latency messages but deliberately does not support generation. */
public class ExtractOnlyJsonBasedLatencyMonitoringMessageAdaptor
    extends JsonBasedLatencyMonitoringMessageAdaptor {

  @Override
  protected String getRandomPayload() {
    throw new ImproperUsageException("Extract-only adaptor cannot generate messages.");
  }
}
