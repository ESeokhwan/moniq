package moniq.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import moniq.exception.ImproperUsageException;
import moniq.exception.InvalidMessageException;
import org.junit.jupiter.api.Test;

class FastJsonBasedLatencyMonitoringMessageAdaptorTest {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @Test
  void keepsPayloadOutsideJsonMetadata() throws Exception {
    FastJsonBasedLatencyMonitoringMessageGenerator generator =
        new FastJsonBasedLatencyMonitoringMessageGenerator(24, 64);

    String message = generator.generate("message!1", 123_456L, Map.of("source", "a!b"));
    int dividerIndex = message.lastIndexOf('!');
    JsonNode metadata = OBJECT_MAPPER.readTree(message.substring(0, dividerIndex));
    String payload = message.substring(dividerIndex + 1);

    assertEquals("message!1", metadata.get("id").textValue());
    assertEquals("123456", metadata.get("requested_at").textValue());
    assertEquals("a!b", metadata.get("source").textValue());
    assertFalse(metadata.has("payload"));
    assertEquals(24, payload.length());
    assertEquals("message!1", generator.extractMessageId(message));
    assertEquals(123_456L, generator.extractRequestedAt(message));
    assertEquals("a!b", generator.extractOtherKeyValue(message, "source"));
  }

  @Test
  void acceptsMetadataWithoutPayloadDelimiter() {
    FastExtractOnlyJsonBasedLatencyMonitoringMessageAdaptor adaptor =
        new FastExtractOnlyJsonBasedLatencyMonitoringMessageAdaptor();
    String metadata = "{\"id\":\"message-1\",\"requested_at\":123}";

    assertEquals("message-1", adaptor.extractMessageId(metadata));
    assertEquals(123L, adaptor.extractRequestedAt(metadata));
  }

  @Test
  void generatesCurrentEpochMillisWhenTimestampIsOmitted() {
    FastJsonBasedLatencyMonitoringMessageGenerator generator =
        new FastJsonBasedLatencyMonitoringMessageGenerator(0, 0);
    long before = System.currentTimeMillis();

    String message = generator.generate("message-1");

    long after = System.currentTimeMillis();
    long requestedAt = generator.extractRequestedAt(message);
    assertTrue(requestedAt >= before && requestedAt <= after);
    assertTrue(message.endsWith("!"));
  }

  @Test
  void extractOnlyAdaptorRejectsGenerationAndInvalidMetadata() {
    FastExtractOnlyJsonBasedLatencyMonitoringMessageAdaptor adaptor =
        new FastExtractOnlyJsonBasedLatencyMonitoringMessageAdaptor();

    assertThrows(ImproperUsageException.class, () -> adaptor.generate("message-1"));
    assertThrows(InvalidMessageException.class, () -> adaptor.extractMessageId("not-json!data"));
  }

  @Test
  void rejectsReservedMetadataAndInvalidGeneratorConfiguration() {
    FastJsonBasedLatencyMonitoringMessageGenerator generator =
        new FastJsonBasedLatencyMonitoringMessageGenerator(10, 10);

    assertThrows(
        IllegalArgumentException.class,
        () -> generator.generate("message-1", 1L, Map.of("payload", "replacement")));
    assertThrows(
        IllegalArgumentException.class,
        () -> new FastJsonBasedLatencyMonitoringMessageGenerator(-1, 10));
    assertThrows(
        IllegalArgumentException.class,
        () -> new FastJsonBasedLatencyMonitoringMessageGenerator(10, -1));
  }
}
