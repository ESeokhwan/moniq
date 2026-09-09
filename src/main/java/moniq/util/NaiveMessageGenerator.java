package moniq.util;

public class NaiveMessageGenerator extends NaiveMessageAdaptor {

  private final int messageSize;

  private final IPayloadGenerator payloadGenerator;

  /**
   * Creates a generator whose output has {@code messageSize} Java characters. Because padding is
   * ASCII but content may not be, this value is not guaranteed to equal the UTF-8 byte length.
   */
  public NaiveMessageGenerator(int messageSize, IPayloadGenerator payloadGenerator) {
    super();
    if (messageSize <= 0) {
      throw new IllegalArgumentException("messageSize must be greater than zero");
    }
    this.messageSize = messageSize;
    this.payloadGenerator = payloadGenerator;
  }

  /**
   * Creates a generator whose output has {@code messageSize} Java characters. Because padding is
   * ASCII but content may not be, this value is not guaranteed to equal the UTF-8 byte length.
   */
  @Deprecated
  public NaiveMessageGenerator(int messageSize, int preIndicesSize) {
    this(messageSize, new BasicRandomPayloadGenerator(Math.min(messageSize, preIndicesSize)));
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
    return payloadGenerator.generatePayload(paddingSize);
  }
}
