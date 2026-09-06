package moniq.util;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.Map;
import java.util.Objects;

public abstract class FastJsonBasedLatencyMonitoringMessageAdaptor
    implements ILatencyMonitoringMessageAdaptor {

  private static final char DIVIDER = '!';

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
    String payload = Objects.requireNonNull(getRandomPayload(), "payload must not be null");
    String metadata =
        LatencyMessageJsonSupport.createMessage(
            messageId, requestedAt, payload, otherKeyValues, false);
    return metadata + DIVIDER + payload;
  }

  @Override
  public String extractMessageId(String message) {
    JsonNode root = LatencyMessageJsonSupport.parseObject(getPayloadRemoved(message));
    return LatencyMessageJsonSupport.extractRequiredText(root, LatencyMessageJsonSupport.ID_KEY);
  }

  @Override
  public long extractRequestedAt(String message) {
    return LatencyMessageJsonSupport.extractRequestedAt(
        LatencyMessageJsonSupport.parseObject(getPayloadRemoved(message)));
  }

  public String extractOtherKeyValue(String message, String key) {
    return LatencyMessageJsonSupport.extractRequiredText(
        LatencyMessageJsonSupport.parseObject(getPayloadRemoved(message)), key);
  }

  protected String getPayloadRemoved(String message) {
    Objects.requireNonNull(message, "message must not be null");
    int dividerIndex = message.lastIndexOf(DIVIDER);
    return dividerIndex < 0 ? message : message.substring(0, dividerIndex);
  }
}
