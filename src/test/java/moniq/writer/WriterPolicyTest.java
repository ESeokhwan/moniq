package moniq.writer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Duration;
import org.junit.jupiter.api.Test;

class WriterPolicyTest {

  @Test
  void fixedBatchPolicyRequiresAPositiveSize() {
    BatchPolicy.FixedSize policy = BatchPolicy.fixedSize(100);

    assertEquals(100, policy.size());
    assertThrows(IllegalArgumentException.class, () -> BatchPolicy.fixedSize(0));
    assertThrows(IllegalArgumentException.class, () -> BatchPolicy.fixedSize(-1));
  }

  @Test
  void unboundedBatchPolicyIsExplicitAndReusable() {
    assertInstanceOf(BatchPolicy.Unbounded.class, BatchPolicy.unbounded());
    assertSame(BatchPolicy.unbounded(), BatchPolicy.unbounded());
  }

  @Test
  void timedFlushPolicyRequiresAPositiveDuration() {
    FlushPolicy.After policy = FlushPolicy.after(Duration.ofSeconds(2));

    assertEquals(Duration.ofSeconds(2), policy.timeout());
    assertThrows(NullPointerException.class, () -> FlushPolicy.after(null));
    assertThrows(IllegalArgumentException.class, () -> FlushPolicy.after(Duration.ZERO));
    assertThrows(
        IllegalArgumentException.class, () -> FlushPolicy.after(Duration.ofMillis(-1)));
    assertThrows(
        IllegalArgumentException.class,
        () -> FlushPolicy.after(Duration.ofSeconds(Long.MAX_VALUE)));
  }

  @Test
  void disabledFlushPolicyIsExplicitAndReusable() {
    assertInstanceOf(FlushPolicy.Disabled.class, FlushPolicy.disabled());
    assertSame(FlushPolicy.disabled(), FlushPolicy.disabled());
  }
}
