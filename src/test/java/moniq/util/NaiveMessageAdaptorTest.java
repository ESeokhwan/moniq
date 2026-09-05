package moniq.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

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
    ExtractOnlyNaiveMessageAdaptor adaptor = new ExtractOnlyNaiveMessageAdaptor(32);

    assertEquals("message-1", adaptor.extractMessageId("message-1!padding"));
    assertThrows(RuntimeException.class, () -> adaptor.generate("message-1"));
  }
}
