package moniq.util;

import java.util.Objects;

public abstract class NaiveMessageAdaptor implements IMessageAdaptor {

  private final static char divChar = '!';

  protected NaiveMessageAdaptor() {
  }

  abstract String getRandomPadding(String content);

  @Override
  public String generate(String content) {
    Objects.requireNonNull(content, "content must not be null");
    String padding = getRandomPadding(content);
    return content + divChar + padding;
  }

  @Override
  public String extractMessageId(String message) {
    int messageIdSize = message.lastIndexOf(divChar);
    if (messageIdSize < 0) {
      messageIdSize = message.length();
    }

    return message.substring(0, messageIdSize);
  }
}
