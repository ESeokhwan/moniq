package moniq.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import moniq.exception.ImproperUsageException;
import moniq.exception.InvalidMessageException;
import org.junit.jupiter.api.Test;

class JsonBasedLatencyMonitoringMessageAdaptorTest {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @Test
  void generatesJsonWithPayloadTimestampAndAdditionalMetadata() throws Exception {
    JsonBasedLatencyMonitoringMessageGenerator generator =
        new JsonBasedLatencyMonitoringMessageGenerator(24, 64);

    String message =
        generator.generate("message-\"1", 123_456L, Map.of("source", "a\\b"));
    JsonNode json = OBJECT_MAPPER.readTree(message);

    assertEquals("message-\"1", json.get("id").textValue());
    assertEquals("123456", json.get("requested_at").textValue());
    assertEquals(24, json.get("payload").textValue().length());
    assertEquals("a\\b", json.get("source").textValue());
    assertEquals("message-\"1", generator.extractMessageId(message));
    assertEquals(123_456L, generator.extractRequestedAt(message));
    assertEquals("a\\b", generator.extractOtherKeyValue(message, "source"));
  }

  @Test
  void generatesCurrentEpochMillisWhenTimestampIsOmitted() {
    JsonBasedLatencyMonitoringMessageGenerator generator =
        new JsonBasedLatencyMonitoringMessageGenerator(0, 0);
    long before = System.currentTimeMillis();

    String message = generator.generate("message-1");

    long after = System.currentTimeMillis();
    long requestedAt = generator.extractRequestedAt(message);
    assertTrue(requestedAt >= before && requestedAt <= after);
  }

  @Test
  void acceptsNumericRequestedAtForCppInteroperability() {
    ExtractOnlyJsonBasedLatencyMonitoringMessageAdaptor adaptor =
        new ExtractOnlyJsonBasedLatencyMonitoringMessageAdaptor();

    assertEquals(123L, adaptor.extractRequestedAt("{\"id\":\"1\",\"requested_at\":123}"));
  }

  @Test
  void rejectsInvalidOrIncompleteMessages() {
    ExtractOnlyJsonBasedLatencyMonitoringMessageAdaptor adaptor =
        new ExtractOnlyJsonBasedLatencyMonitoringMessageAdaptor();

    assertThrows(InvalidMessageException.class, () -> adaptor.extractMessageId("not-json"));
    assertThrows(InvalidMessageException.class, () -> adaptor.extractMessageId("[]"));
    assertThrows(InvalidMessageException.class, () -> adaptor.extractMessageId("{}"));
    assertThrows(
        InvalidMessageException.class,
        () -> adaptor.extractRequestedAt("{\"requested_at\":\"not-a-number\"}"));
  }

  @Test
  void extractOnlyAdaptorRejectsGeneration() {
    ExtractOnlyJsonBasedLatencyMonitoringMessageAdaptor adaptor =
        new ExtractOnlyJsonBasedLatencyMonitoringMessageAdaptor();

    assertThrows(ImproperUsageException.class, () -> adaptor.generate("message-1"));
  }

  @Test
  void rejectsReservedOrNullAdditionalMetadata() {
    JsonBasedLatencyMonitoringMessageGenerator generator =
        new JsonBasedLatencyMonitoringMessageGenerator(10, 10);

    assertThrows(
        IllegalArgumentException.class,
        () -> generator.generate("message-1", 1L, Map.of("id", "replacement")));
    assertThrows(
        NullPointerException.class,
        () -> generator.generate("message-1", 1L, null));
  }

  @Test
  void rejectsInvalidGeneratorConfiguration() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new JsonBasedLatencyMonitoringMessageGenerator(-1, 10));
    assertThrows(
        IllegalArgumentException.class,
        () -> new JsonBasedLatencyMonitoringMessageGenerator(10, -1));
  }
}
