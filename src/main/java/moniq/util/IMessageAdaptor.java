package moniq.util;

/** Generates transport messages and extracts their monitoring identifier. */
public interface IMessageAdaptor {

  /** Generates a transport message for the given identifier. */
  String generate(String messageId);

  /** Extracts the monitoring identifier from a transport message. */
  String extractMessageId(String message);
}
