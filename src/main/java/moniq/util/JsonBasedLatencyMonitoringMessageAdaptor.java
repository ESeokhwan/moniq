package moniq.util;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.Map;

/** Base implementation for latency metadata and payload contained in one JSON object. */
public abstract class JsonBasedLatencyMonitoringMessageAdaptor
    implements ILatencyMonitoringMessageAdaptor {

  /** Returns the payload to include in the next generated message. */
  protected abstract String getRandomPayload();

  @Override
  public String generate(String messageId) {
    return generate(messageId, System.currentTimeMillis());
  }

  @Override
  public String generate(String messageId, long requestedAt) {
    return generate(messageId, requestedAt, Map.of());
  }

  public String generate(String messageId, Map<String, String> otherKeyValues) {
    return generate(messageId, System.currentTimeMillis(), otherKeyValues);
  }

  public String generate(
      String messageId, long requestedAt, Map<String, String> otherKeyValues) {
    return LatencyMessageJsonSupport.createMessage(
        messageId, requestedAt, getRandomPayload(), otherKeyValues, true);
  }

  @Override
  public String extractMessageId(String message) {
    JsonNode root = LatencyMessageJsonSupport.parseObject(message);
    return LatencyMessageJsonSupport.extractRequiredText(root, LatencyMessageJsonSupport.ID_KEY);
  }

  @Override
  public long extractRequestedAt(String message) {
    return LatencyMessageJsonSupport.extractRequestedAt(
        LatencyMessageJsonSupport.parseObject(message));
  }

  public String extractOtherKeyValue(String message, String key) {
    return LatencyMessageJsonSupport.extractRequiredText(
        LatencyMessageJsonSupport.parseObject(message), key);
  }
}
