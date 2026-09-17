package moniq.writer.strategy;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import moniq.IMonitorLog;

/**
 * Writes each log to multiple strategies in a deterministic order.
 *
 * <p>This is a fan-out strategy, not a transaction: a preceding strategy may successfully write a
 * log before a later strategy fails. In that case {@link #write(IMonitorLog)} stops immediately and
 * throws {@link WriteException}; callers should still commit the batch to flush preceding output.
 *
 * <p>{@link #commit()} calls every child strategy even when an earlier child returns {@code false}.
 * Its result is {@code true} only when every child commits successfully. Runtime commit failures
 * are reported after the remaining children have been given a chance to commit.
 *
 * <p>The normal {@code MonitorLogWriter} calls this strategy from one writer thread. Do not invoke
 * this strategy or any child strategy concurrently; close it after the writer has stopped.
 */
public final class CompositeMonitorLogWriteStrategy
    implements IMonitorLogWriteStrategy, AutoCloseable {

  private final List<IMonitorLogWriteStrategy> strategies;
  private boolean closed;

  public CompositeMonitorLogWriteStrategy(IMonitorLogWriteStrategy... strategies) {
    this(List.of(Objects.requireNonNull(strategies, "strategies must not be null")));
  }

  public CompositeMonitorLogWriteStrategy(
      List<? extends IMonitorLogWriteStrategy> strategies) {
    Objects.requireNonNull(strategies, "strategies must not be null");
    if (strategies.isEmpty()) {
      throw new IllegalArgumentException("strategies must not be empty");
    }
    List<IMonitorLogWriteStrategy> copied = new ArrayList<>(strategies.size());
    for (IMonitorLogWriteStrategy strategy : strategies) {
      copied.add(Objects.requireNonNull(strategy, "strategy must not be null"));
    }
    this.strategies = List.copyOf(copied);
  }

  /** Returns the immutable, invocation-ordered child strategies. */
  public List<IMonitorLogWriteStrategy> strategies() {
    return strategies;
  }

  @Override
  public void write(IMonitorLog log) {
    ensureOpen();
    Objects.requireNonNull(log, "log must not be null");
    for (int i = 0; i < strategies.size(); i++) {
      IMonitorLogWriteStrategy strategy = strategies.get(i);
      try {
        strategy.write(log);
      } catch (RuntimeException error) {
        throw new WriteException(i, strategy, error);
      }
    }
  }

  @Override
  public boolean commit() {
    ensureOpen();
    boolean committed = true;
    RuntimeException failure = null;
    for (int i = 0; i < strategies.size(); i++) {
      IMonitorLogWriteStrategy strategy = strategies.get(i);
      try {
        committed &= strategy.commit();
      } catch (RuntimeException error) {
        if (failure == null) {
          failure = new CommitException(i, strategy, error);
        } else {
          failure.addSuppressed(new CommitException(i, strategy, error));
        }
      }
    }
    if (failure != null) {
      throw failure;
    }
    return committed;
  }

  /**
   * Closes closeable children in reverse order. Repeated calls are harmless.
   *
   * <p>The caller must close the composite only after its {@code MonitorLogWriter} has stopped.
   */
  @Override
  public void close() {
    if (closed) {
      return;
    }
    closed = true;

    RuntimeException failure = null;
    for (int i = strategies.size() - 1; i >= 0; i--) {
      IMonitorLogWriteStrategy strategy = strategies.get(i);
      if (!(strategy instanceof AutoCloseable closeable)) {
        continue;
      }
      try {
        closeable.close();
      } catch (Exception error) {
        if (failure == null) {
          failure = new CloseException(i, strategy, error);
        } else {
          failure.addSuppressed(new CloseException(i, strategy, error));
        }
      }
    }
    if (failure != null) {
      throw failure;
    }
  }

  private void ensureOpen() {
    if (closed) {
      throw new IllegalStateException("CompositeMonitorLogWriteStrategy is closed");
    }
  }

  /** Identifies the child strategy that failed while writing. */
  public static final class WriteException extends RuntimeException {
    private final int strategyIndex;
    private final IMonitorLogWriteStrategy strategy;

    private WriteException(int strategyIndex, IMonitorLogWriteStrategy strategy, RuntimeException cause) {
      super("Write strategy at index " + strategyIndex + " failed: " + strategy.getClass().getName(), cause);
      this.strategyIndex = strategyIndex;
      this.strategy = strategy;
    }

    public int strategyIndex() {
      return strategyIndex;
    }

    public IMonitorLogWriteStrategy strategy() {
      return strategy;
    }
  }

  /** Identifies the child strategy that failed while committing. */
  public static final class CommitException extends RuntimeException {
    private final int strategyIndex;
    private final IMonitorLogWriteStrategy strategy;

    private CommitException(int strategyIndex, IMonitorLogWriteStrategy strategy, RuntimeException cause) {
      super("Write strategy at index " + strategyIndex + " failed to commit: "
          + strategy.getClass().getName(), cause);
      this.strategyIndex = strategyIndex;
      this.strategy = strategy;
    }

    public int strategyIndex() {
      return strategyIndex;
    }

    public IMonitorLogWriteStrategy strategy() {
      return strategy;
    }
  }

  /** Identifies the child strategy that failed while closing. */
  public static final class CloseException extends RuntimeException {
    private final int strategyIndex;
    private final IMonitorLogWriteStrategy strategy;

    private CloseException(int strategyIndex, IMonitorLogWriteStrategy strategy, Exception cause) {
      super("Write strategy at index " + strategyIndex + " failed to close: "
          + strategy.getClass().getName(), cause);
      this.strategyIndex = strategyIndex;
      this.strategy = strategy;
    }

    public int strategyIndex() {
      return strategyIndex;
    }

    public IMonitorLogWriteStrategy strategy() {
      return strategy;
    }
  }
}
