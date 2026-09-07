package moniq.writer;

/** Defines when the number of pending logs makes a writer batch ready. */
public sealed interface BatchPolicy permits BatchPolicy.FixedSize, BatchPolicy.Unbounded {

  /** Creates a policy that flushes whenever {@code size} logs accumulate. */
  static FixedSize fixedSize(int size) {
    return new FixedSize(size);
  }

  /** Creates a policy with no size-based flush boundary. */
  static Unbounded unbounded() {
    return Unbounded.INSTANCE;
  }

  /** A positive, fixed-size batch boundary. */
  record FixedSize(int size) implements BatchPolicy {

    public FixedSize {
      if (size <= 0) {
        throw new IllegalArgumentException("batch size must be greater than zero");
      }
    }
  }

  /** A batch with no size boundary; timeout or shutdown determines when it is flushed. */
  enum Unbounded implements BatchPolicy {
    INSTANCE
  }
}
