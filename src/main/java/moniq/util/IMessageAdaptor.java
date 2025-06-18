package moniq.util;

public interface IMessageAdaptor {
  String generate(String messageId);

  String extractMessageId(String message);
}
