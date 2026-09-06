package moniq.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import moniq.exception.InvalidMessageException;

final class LatencyMessageJsonSupport {

  static final String ID_KEY = "id";
  static final String REQUESTED_AT_KEY = "requested_at";
  static final String PAYLOAD_KEY = "payload";

  private static final Set<String> RESERVED_KEYS =
      Set.of(ID_KEY, REQUESTED_AT_KEY, PAYLOAD_KEY);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private LatencyMessageJsonSupport() {
  }

  static String createMessage(
      String messageId,
      long requestedAt,
      String payload,
      Map<String, String> otherKeyValues,
      boolean includePayload) {
    Objects.requireNonNull(messageId, "messageId must not be null");
    Objects.requireNonNull(payload, "payload must not be null");
    Objects.requireNonNull(otherKeyValues, "otherKeyValues must not be null");

    ObjectNode message = OBJECT_MAPPER.createObjectNode();
    message.put(ID_KEY, messageId);
    message.put(REQUESTED_AT_KEY, Long.toString(requestedAt));
    if (includePayload) {
      message.put(PAYLOAD_KEY, payload);
    }

    for (Map.Entry<String, String> entry : otherKeyValues.entrySet()) {
      String key = Objects.requireNonNull(entry.getKey(), "metadata key must not be null");
      String value = Objects.requireNonNull(entry.getValue(), "metadata value must not be null");
      if (RESERVED_KEYS.contains(key)) {
        throw new IllegalArgumentException("metadata key is reserved: " + key);
      }
      message.put(key, value);
    }

    try {
      return OBJECT_MAPPER.writeValueAsString(message);
    } catch (JsonProcessingException e) {
      throw new InvalidMessageException("Failed to generate a latency monitoring message.", e);
    }
  }

  static JsonNode parseObject(String message) {
    Objects.requireNonNull(message, "message must not be null");
    try {
      JsonNode root = OBJECT_MAPPER.readTree(message);
      if (root == null || !root.isObject()) {
        throw new InvalidMessageException("Latency monitoring message must be a JSON object.");
      }
      return root;
    } catch (JsonProcessingException e) {
      throw new InvalidMessageException("Invalid latency monitoring JSON message.", e);
    }
  }

  static String extractRequiredText(JsonNode root, String key) {
    JsonNode value = root.get(key);
    if (value == null || !value.isTextual()) {
      throw new InvalidMessageException("Missing or non-string field: " + key);
    }
    return value.textValue();
  }

  static long extractRequestedAt(JsonNode root) {
    JsonNode value = root.get(REQUESTED_AT_KEY);
    if (value == null) {
      throw new InvalidMessageException("Missing field: " + REQUESTED_AT_KEY);
    }

    if (value.isIntegralNumber() && value.canConvertToLong()) {
      return value.longValue();
    }
    if (value.isTextual()) {
      try {
        return Long.parseLong(value.textValue());
      } catch (NumberFormatException e) {
        throw new InvalidMessageException(
            "Field must contain a 64-bit integer: " + REQUESTED_AT_KEY, e);
      }
    }
    throw new InvalidMessageException(
        "Field must be an integer or integer string: " + REQUESTED_AT_KEY);
  }
}
