package moniq.util;

public interface ILatencyMonitoringMessageAdaptor extends IMessageAdaptor {

  String generate(String messageId, long requestedAt);

  long extractRequestedAt(String message);
}
