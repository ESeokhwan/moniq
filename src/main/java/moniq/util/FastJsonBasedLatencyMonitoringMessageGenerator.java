package moniq.util;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

/** Generates fast JSON latency messages with a pre-generated random payload pool. */
public class FastJsonBasedLatencyMonitoringMessageGenerator
    extends FastJsonBasedLatencyMonitoringMessageAdaptor {

  private final int payloadSize;
  private final IPayloadGenerator payloadGenerator;

  /**
   * Creates a generator with a payload of {@code payloadSize} Java characters. The complete message
   * consists of compact JSON metadata, one delimiter, and the payload.
   *
   * @param payloadSize number of alphanumeric payload characters
   * @param payloadGenerator payload generator
   */
  public FastJsonBasedLatencyMonitoringMessageGenerator(int payloadSize, IPayloadGenerator payloadGenerator) {
    if (payloadSize < 0) {
      throw new IllegalArgumentException("payloadSize must not be negative");
    }
    this.payloadSize = payloadSize;
    this.payloadGenerator = payloadGenerator;
  }

  /**
   * Creates a generator with a payload of {@code payloadSize} Java characters. The complete message
   * consists of compact JSON metadata, one delimiter, and the payload.
   *
   * @param payloadSize number of alphanumeric payload characters
   * @param preIndicesSize minimum size of the reusable random-character pool
   */
  @Deprecated
  public FastJsonBasedLatencyMonitoringMessageGenerator(int payloadSize, int preIndicesSize) {
    this(payloadSize, new BasicRandomPayloadGenerator(Math.min(payloadSize, preIndicesSize)));
  }

  @Override
  protected String getRandomPayload() {
    if (payloadSize == 0) {
      return "";
    }
    return payloadGenerator.generatePayload(payloadSize);
  }
}
