package moniq.util;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

public class NaiveMessageGenerator extends NaiveMessageAdaptor {

  private final static String paddingCharacters = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

  private final int messageSize;

  private final String preGeneratedPayload;

  private final AtomicInteger curIdx = new AtomicInteger(0);

  /**
   * Creates a generator whose output has {@code messageSize} Java characters. Because padding is
   * ASCII but content may not be, this value is not guaranteed to equal the UTF-8 byte length.
   */
  public NaiveMessageGenerator(int messageSize, int preIndicesSize) {
    super();
    if (messageSize <= 0) {
      throw new IllegalArgumentException("messageSize must be greater than zero");
    }
    if (preIndicesSize < 0) {
      throw new IllegalArgumentException("preIndicesSize must not be negative");
    }
    this.messageSize = messageSize;
    this.preGeneratedPayload = generatePayloadPool(preIndicesSize);
  }

  private String generatePayloadPool(int preIndicesSize) {
    int poolSize = Math.max(preIndicesSize, messageSize);
    StringBuilder payload = new StringBuilder(poolSize);

    for (int i = 0; i < poolSize; i++) {
      int randomIndex = ThreadLocalRandom.current().nextInt(paddingCharacters.length());
      payload.append(paddingCharacters.charAt(randomIndex));
    }
    return payload.toString();
  }

  @Override
  protected String getRandomPadding(String content) {
    int paddingSize = messageSize - content.length() - 1;
    if (paddingSize < 0) {
      throw new IllegalArgumentException(
          "content and delimiter must fit within messageSize: " + messageSize);
    }
    if (paddingSize == 0) {
      return "";
    }

    int windowCount = preGeneratedPayload.length() - paddingSize + 1;
    int startIndex = Math.floorMod(curIdx.getAndIncrement(), windowCount);

    return preGeneratedPayload.substring(startIndex, startIndex + paddingSize);
  }
}
