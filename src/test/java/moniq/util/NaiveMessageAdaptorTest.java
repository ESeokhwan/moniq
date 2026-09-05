package moniq.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import moniq.exception.ImproperUsageException;
import org.junit.jupiter.api.Test;

class NaiveMessageAdaptorTest {

  @Test
  void generatesFixedLengthMessageAndExtractsItsId() {
    NaiveMessageGenerator generator = new NaiveMessageGenerator(32, 64);

    String message = generator.generate("message-1");

    assertEquals(32, message.length());
    assertEquals("message-1", generator.extractMessageId(message));
  }

  @Test
  void extractOnlyAdaptorRejectsGeneration() {
    ExtractOnlyNaiveMessageAdaptor adaptor = new ExtractOnlyNaiveMessageAdaptor();

    assertEquals("message-1", adaptor.extractMessageId("message-1!padding"));
    assertThrows(ImproperUsageException.class, () -> adaptor.generate("message-1"));
  }

  @Test
  @SuppressWarnings("deprecation")
  void keepsLegacyExtractOnlyConstructorCompatible() {
    ExtractOnlyNaiveMessageAdaptor adaptor = new ExtractOnlyNaiveMessageAdaptor(32);

    assertEquals("message-1", adaptor.extractMessageId("message-1!padding"));
  }

  @Test
  void preservesDelimiterCharactersInsideTheContent() {
    NaiveMessageGenerator generator = new NaiveMessageGenerator(32, 64);

    String message = generator.generate("message!1");

    assertEquals("message!1", generator.extractMessageId(message));
  }

  @Test
  void rejectsInvalidGeneratorConfigurationAndOversizedContent() {
    assertThrows(IllegalArgumentException.class, () -> new NaiveMessageGenerator(0, 10));
    assertThrows(IllegalArgumentException.class, () -> new NaiveMessageGenerator(10, -1));

    NaiveMessageGenerator generator = new NaiveMessageGenerator(4, 10);
    assertThrows(IllegalArgumentException.class, () -> generator.generate("four"));
  }

  @Test
  void supportsConcurrentGeneration() throws Exception {
    NaiveMessageGenerator generator = new NaiveMessageGenerator(128, 512);
    ExecutorService executor = Executors.newFixedThreadPool(8);
    List<Callable<Boolean>> tasks = new ArrayList<>();
    for (int i = 0; i < 1_000; i++) {
      String id = "message-" + i;
      tasks.add(
          () -> {
            String message = generator.generate(id);
            return message.length() == 128 && id.equals(generator.extractMessageId(message));
          });
    }

    try {
      List<Future<Boolean>> results = executor.invokeAll(tasks);
      for (Future<Boolean> result : results) {
        assertTrue(result.get());
      }
    } finally {
      executor.shutdownNow();
    }
  }
}
