package moniq.util;

/** A message adaptor that carries an epoch-millisecond request timestamp. */
public interface ILatencyMonitoringMessageAdaptor extends IMessageAdaptor {

  /** Generates a message with an explicit request timestamp. */
  String generate(String messageId, long requestedAt);

  /** Extracts the epoch-millisecond request timestamp. */
  long extractRequestedAt(String message);
}
