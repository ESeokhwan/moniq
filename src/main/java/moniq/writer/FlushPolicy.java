package moniq.writer;

import java.time.Duration;
import java.util.Objects;

/** Defines whether pending logs are flushed after a time limit. */
public sealed interface FlushPolicy permits FlushPolicy.After, FlushPolicy.Disabled {

  /** Creates a policy with no time-based flush boundary. */
  static Disabled disabled() {
    return Disabled.INSTANCE;
  }

  /** Creates a policy that flushes pending logs after a positive duration. */
  static After after(Duration timeout) {
    return new After(timeout);
  }

  /** A positive time-based flush boundary. */
  record After(Duration timeout) implements FlushPolicy {

    public After {
      Objects.requireNonNull(timeout, "timeout must not be null");
      if (timeout.isZero() || timeout.isNegative()) {
        throw new IllegalArgumentException("timeout must be greater than zero");
      }
      try {
        timeout.toNanos();
      } catch (ArithmeticException e) {
        throw new IllegalArgumentException("timeout is too large", e);
      }
    }
  }

  /** No time-based flush boundary. */
  enum Disabled implements FlushPolicy {
    INSTANCE
  }
}
