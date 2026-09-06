package moniq.util;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

public class JsonBasedLatencyMonitoringMessageGenerator
    extends JsonBasedLatencyMonitoringMessageAdaptor {

  private static final String PAYLOAD_CHARACTERS =
      "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

  private final int payloadSize;
  private final String preGeneratedPayload;
  private final AtomicInteger currentIndex = new AtomicInteger();

  /**
   * Creates a generator with a payload of {@code payloadSize} Java characters. The complete JSON
   * message length varies with its metadata and JSON escaping.
   */
  public JsonBasedLatencyMonitoringMessageGenerator(int payloadSize, int preIndicesSize) {
    if (payloadSize < 0) {
      throw new IllegalArgumentException("payloadSize must not be negative");
    }
    if (preIndicesSize < 0) {
      throw new IllegalArgumentException("preIndicesSize must not be negative");
    }
    this.payloadSize = payloadSize;
    this.preGeneratedPayload = generatePayloadPool(preIndicesSize);
  }

  private String generatePayloadPool(int preIndicesSize) {
    int poolSize = Math.max(preIndicesSize, payloadSize);
    StringBuilder payload = new StringBuilder(poolSize);
    for (int i = 0; i < poolSize; i++) {
      int randomIndex = ThreadLocalRandom.current().nextInt(PAYLOAD_CHARACTERS.length());
      payload.append(PAYLOAD_CHARACTERS.charAt(randomIndex));
    }
    return payload.toString();
  }

  @Override
  protected String getRandomPayload() {
    if (payloadSize == 0) {
      return "";
    }
    int windowCount = preGeneratedPayload.length() - payloadSize + 1;
    int startIndex = Math.floorMod(currentIndex.getAndIncrement(), windowCount);
    return preGeneratedPayload.substring(startIndex, startIndex + payloadSize);
  }
}
