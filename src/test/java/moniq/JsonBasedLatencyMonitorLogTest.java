package moniq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import moniq.exception.InvalidMessageException;
import moniq.exception.NotProcessedException;
import moniq.util.ExtractOnlyJsonBasedLatencyMonitoringMessageAdaptor;
import moniq.util.FastExtractOnlyJsonBasedLatencyMonitoringMessageAdaptor;
import moniq.util.JsonBasedLatencyMonitoringMessageGenerator;
import org.junit.jupiter.api.Test;

class JsonBasedLatencyMonitorLogTest {

  @Test
  void exposesRawValuesButRequiresPreprocessingForExtractedValues() {
    JsonBasedLatencyMonitorLog log =
        new JsonBasedLatencyMonitorLog(
            new ExtractOnlyJsonBasedLatencyMonitoringMessageAdaptor(),
            "{\"id\":\"message-1\",\"requested_at\":100}",
            "success",
            135L);

    assertEquals("{\"id\":\"message-1\",\"requested_at\":100}", log.getRawData());
    assertEquals("success", log.getStatus());
    assertEquals(135L, log.getRespondedAt());
    assertEquals(
        List.of("Content", "Status", "RequestedAt", "RespondedAt", "Latency"),
        log.getHeaders());
    assertThrows(NotProcessedException.class, log::getContent);
    assertThrows(NotProcessedException.class, log::getRequestedAt);
    assertThrows(NotProcessedException.class, log::getLatency);
    assertThrows(NotProcessedException.class, log::getValues);
  }

  @Test
  void preprocessesJsonMessageAndCalculatesLatency() {
    JsonBasedLatencyMonitoringMessageGenerator generator =
        new JsonBasedLatencyMonitoringMessageGenerator(16, 32);
    String message = generator.generate("message-1", 100L);
    JsonBasedLatencyMonitorLog log =
        new JsonBasedLatencyMonitorLog(generator, message, "success", 135L);

    log.preprocess();

    assertEquals("message-1", log.getContent());
    assertEquals(100L, log.getRequestedAt());
    assertEquals(135L, log.getRespondedAt());
    assertEquals(35L, log.getLatency());
    assertEquals(List.of("message-1", "success", "100", "135", "35"), log.getValues());
  }

  @Test
  void preprocessesFastJsonMessageThroughTheCommonAdaptorContract() {
    String message = "{\"id\":\"message-2\",\"requested_at\":200}!payload";
    JsonBasedLatencyMonitorLog log =
        new JsonBasedLatencyMonitorLog(
            new FastExtractOnlyJsonBasedLatencyMonitoringMessageAdaptor(),
            message,
            "failure",
            190L);

    log.preprocess();

    assertEquals(List.of("message-2", "failure", "200", "190", "-10"), log.getValues());
  }

  @Test
  void doesNotPublishPartiallyExtractedValuesWhenPreprocessingFails() {
    JsonBasedLatencyMonitorLog log =
        new JsonBasedLatencyMonitorLog(
            new ExtractOnlyJsonBasedLatencyMonitoringMessageAdaptor(),
            "{\"id\":\"message-1\"}",
            "failure",
            135L);

    assertThrows(InvalidMessageException.class, log::preprocess);
    assertThrows(NotProcessedException.class, log::getContent);
  }

  @Test
  void rejectsNullConstructorArguments() {
    ExtractOnlyJsonBasedLatencyMonitoringMessageAdaptor adaptor =
        new ExtractOnlyJsonBasedLatencyMonitoringMessageAdaptor();

    assertThrows(
        NullPointerException.class,
        () -> new JsonBasedLatencyMonitorLog(null, "{}", "success", 1L));
    assertThrows(
        NullPointerException.class,
        () -> new JsonBasedLatencyMonitorLog(adaptor, null, "success", 1L));
    assertThrows(
        NullPointerException.class,
        () -> new JsonBasedLatencyMonitorLog(adaptor, "{}", null, 1L));
  }
}
