package moniq.writer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import moniq.IMonitorLog;
import moniq.MonitorQueue;
import moniq.writer.strategy.IMonitorLogWriteStrategy;

/**
 * A one-shot queue consumer that preprocesses logs and writes them in FIFO batches.
 *
 * <p>Use {@link #submit(IMonitorLog)} for enqueueing without a shared producer lock. Call {@link
 * #gracefulShutdown()} and join the owning thread to drain accepted submissions and close the
 * writer lifecycle.
 */
public class MonitorLogWriter implements Runnable {

  private static final AtomicInteger POOL_SEQUENCE = new AtomicInteger();

  private final MonitorQueue monitorQueue;
  private final IMonitorLogWriteStrategy writeStrategy;
  private final BatchPolicy batchPolicy;
  private final long flushTimeoutNanos;
  private final MonitorLogErrorHandler errorHandler;
  private final ExecutorService preprocessingExecutor;

  private final AtomicBoolean accepting = new AtomicBoolean(true);
  private final AtomicInteger inFlightSubmissions = new AtomicInteger();
  private final AtomicBoolean started = new AtomicBoolean();
  private final AtomicBoolean wakeupRequested = new AtomicBoolean();
  private final AtomicReference<Thread> writerThread = new AtomicReference<>();
  private final AtomicReference<Thread> legacyWaiter = new AtomicReference<>();

  private long pendingSinceNanos = Long.MIN_VALUE;
  private boolean hasUncommittedWrites;

  /**
   * @deprecated Use {@link #MonitorLogWriter(MonitorQueue, IMonitorLogWriteStrategy, BatchPolicy)}.
   */
  @Deprecated
  public MonitorLogWriter(
      MonitorQueue monitorQueue, IMonitorLogWriteStrategy writeStrategy, int batchSize) {
    this(monitorQueue, writeStrategy, batchSize, Duration.ZERO);
  }

  /**
   * @deprecated Use {@link #MonitorLogWriter(MonitorQueue, IMonitorLogWriteStrategy, BatchPolicy,
   *     FlushPolicy)}.
   */
  @Deprecated
  public MonitorLogWriter(
      MonitorQueue monitorQueue,
      IMonitorLogWriteStrategy writeStrategy,
      int batchSize,
      Duration flushTimeout) {
    this(monitorQueue, writeStrategy, batchSize, flushTimeout, 1);
  }

  /**
   * @deprecated Use {@link #MonitorLogWriter(MonitorQueue, IMonitorLogWriteStrategy, BatchPolicy,
   *     FlushPolicy, int)}.
   */
  @Deprecated
  public MonitorLogWriter(
      MonitorQueue monitorQueue,
      IMonitorLogWriteStrategy writeStrategy,
      int batchSize,
      Duration flushTimeout,
      int workerCount) {
    this(
        monitorQueue,
        writeStrategy,
        batchSize,
        flushTimeout,
        workerCount,
        MonitorLogErrorHandler.rethrowing());
  }

  /**
   * @deprecated Use {@link #MonitorLogWriter(MonitorQueue, IMonitorLogWriteStrategy, BatchPolicy,
   *     FlushPolicy, int, MonitorLogErrorHandler)}.
   */
  @Deprecated
  public MonitorLogWriter(
      MonitorQueue monitorQueue,
      IMonitorLogWriteStrategy writeStrategy,
      int batchSize,
      Duration flushTimeout,
      int workerCount,
      MonitorLogErrorHandler errorHandler) {
    this(
        monitorQueue,
        writeStrategy,
        legacyBatchPolicy(batchSize),
        legacyFlushPolicy(flushTimeout),
        workerCount,
        errorHandler);
  }

  public MonitorLogWriter(
      MonitorQueue monitorQueue,
      IMonitorLogWriteStrategy writeStrategy,
      BatchPolicy batchPolicy) {
    this(monitorQueue, writeStrategy, batchPolicy, FlushPolicy.disabled());
  }

  public MonitorLogWriter(
      MonitorQueue monitorQueue,
      IMonitorLogWriteStrategy writeStrategy,
      BatchPolicy batchPolicy,
      FlushPolicy flushPolicy) {
    this(monitorQueue, writeStrategy, batchPolicy, flushPolicy, 1);
  }

  public MonitorLogWriter(
      MonitorQueue monitorQueue,
      IMonitorLogWriteStrategy writeStrategy,
      BatchPolicy batchPolicy,
      FlushPolicy flushPolicy,
      int workerCount) {
    this(
        monitorQueue,
        writeStrategy,
        batchPolicy,
        flushPolicy,
        workerCount,
        MonitorLogErrorHandler.rethrowing());
  }

  public MonitorLogWriter(
      MonitorQueue monitorQueue,
      IMonitorLogWriteStrategy writeStrategy,
      BatchPolicy batchPolicy,
      FlushPolicy flushPolicy,
      int workerCount,
      MonitorLogErrorHandler errorHandler) {
    this.monitorQueue = Objects.requireNonNull(monitorQueue, "monitorQueue must not be null");
    this.writeStrategy = Objects.requireNonNull(writeStrategy, "writeStrategy must not be null");
    this.batchPolicy = Objects.requireNonNull(batchPolicy, "batchPolicy must not be null");
    this.flushTimeoutNanos = toTimeoutNanos(flushPolicy);
    if (workerCount <= 0) {
      throw new IllegalArgumentException("workerCount must be greater than zero");
    }
    this.errorHandler = Objects.requireNonNull(errorHandler, "errorHandler must not be null");
    this.preprocessingExecutor =
        Executors.newFixedThreadPool(workerCount, newPreprocessingThreadFactory());
  }

  /**
   * Enqueues a log and wakes the writer when a batch is ready or needs a timeout scheduled.
   *
   * @param log log to enqueue
   * @throws IllegalStateException if the writer is no longer accepting submissions
   */
  public void submit(IMonitorLog log) {
    Objects.requireNonNull(log, "log must not be null");
    inFlightSubmissions.incrementAndGet();
    try {
      if (!accepting.get()) {
        throw new IllegalStateException("MonitorLogWriter is shutting down.");
      }
      monitorQueue.enqueue(log);
    } finally {
      inFlightSubmissions.decrementAndGet();
      signalWriter();
    }
  }

  /** Requests shutdown, wakes the writer, and causes all queued logs to be drained. */
  public void gracefulShutdown() {
    accepting.set(false);
    signalWriter();
  }

  /**
   * Parks the current thread until it is interrupted or spuriously awakened.
   *
   * @deprecated This lifecycle primitive is no longer used by the writer. Prefer {@link
   *     #submit(IMonitorLog)}.
   */
  @Deprecated
  public void syncedWait() {
    Thread currentThread = Thread.currentThread();
    if (!legacyWaiter.compareAndSet(null, currentThread)) {
      throw new IllegalStateException("Only one thread can use syncedWait at a time.");
    }
    try {
      LockSupport.park(this);
    } finally {
      legacyWaiter.compareAndSet(currentThread, null);
    }
  }

  /** Wakes the writer thread if it is parked. */
  public void syncedNotify() {
    Thread waiter = legacyWaiter.get();
    if (waiter != null) {
      LockSupport.unpark(waiter);
    }
    signalWriter();
  }

  /** Signals the writer if the externally managed queue is ready or needs a timeout scheduled. */
  public void notifyIfNeeded() {
    signalWriter();
  }

  @Override
  public void run() {
    if (!started.compareAndSet(false, true)) {
      throw new IllegalStateException("MonitorLogWriter can only be run once.");
    }
    Thread currentThread = Thread.currentThread();
    writerThread.set(currentThread);

    try {
      while (true) {
        acknowledgeWakeup();
        if (Thread.interrupted()) {
          accepting.set(false);
        }

        if (!accepting.get()) {
          drainQueue();
          if (canTerminate()) {
            return;
          }
          parkUntilSignal();
          continue;
        }

        if (isBatchReady()) {
          processLogs(fixedBatchSize());
          flushBatch();
          resetPendingTimer();
          continue;
        }

        if (monitorQueue.isEmpty()) {
          pendingSinceNanos = Long.MIN_VALUE;
        } else if (flushTimeoutNanos > 0) {
          markPendingIfNeeded();
          long elapsedNanos = System.nanoTime() - pendingSinceNanos;
          long remainingNanos = flushTimeoutNanos - elapsedNanos;
          if (remainingNanos <= 0) {
            processLogs(monitorQueue.size());
            flushBatch();
            resetPendingTimer();
            continue;
          }
          parkUntilSignal(remainingNanos);
          continue;
        }

        parkUntilSignal();
      }
    } finally {
      accepting.set(false);
      writerThread.compareAndSet(currentThread, null);
      preprocessingExecutor.shutdownNow();
      flushBatch();
    }
  }

  private boolean isBatchReady() {
    return batchPolicy instanceof BatchPolicy.FixedSize fixedSize
        && monitorQueue.size() >= fixedSize.size();
  }

  private void markPendingIfNeeded() {
    if (flushTimeoutNanos > 0
        && pendingSinceNanos == Long.MIN_VALUE
        && !monitorQueue.isEmpty()) {
      pendingSinceNanos = System.nanoTime();
    }
  }

  private void resetPendingTimer() {
    pendingSinceNanos = monitorQueue.isEmpty() ? Long.MIN_VALUE : System.nanoTime();
  }

  private boolean canTerminate() {
    return !accepting.get()
        && inFlightSubmissions.get() == 0
        && monitorQueue.isEmpty();
  }

  private void acknowledgeWakeup() {
    wakeupRequested.set(false);
  }

  private void signalWriter() {
    if (wakeupRequested.compareAndSet(false, true)) {
      Thread thread = writerThread.get();
      if (thread != null) {
        LockSupport.unpark(thread);
      }
    }
  }

  private void parkUntilSignal() {
    if (!wakeupRequested.get()) {
      LockSupport.park(this);
    }
  }

  private void parkUntilSignal(long timeoutNanos) {
    if (!wakeupRequested.get()) {
      LockSupport.parkNanos(this, timeoutNanos);
    }
  }

  private void drainQueue() {
    while (!monitorQueue.isEmpty()) {
      processLogs(
          batchPolicy instanceof BatchPolicy.FixedSize ? fixedBatchSize() : Integer.MAX_VALUE);
      flushBatch();
    }
  }

  private void processLogs(int maximumCount) {
    int initialCapacity = Math.max(0, Math.min(maximumCount, monitorQueue.size()));
    List<IMonitorLog> logs = new ArrayList<>(initialCapacity);
    while (logs.size() < maximumCount) {
      IMonitorLog log = monitorQueue.dequeue();
      if (log == null) {
        break;
      }
      logs.add(log);
    }

    List<Future<?>> preprocessingResults = new ArrayList<>(logs.size());
    for (IMonitorLog log : logs) {
      preprocessingResults.add(preprocessingExecutor.submit(log::preprocess));
    }

    for (int i = 0; i < logs.size(); i++) {
      IMonitorLog log = logs.get(i);
      boolean preprocessed = awaitPreprocessing(log, preprocessingResults.get(i));
      if (preprocessed) {
        try {
          writeStrategy.write(log);
          hasUncommittedWrites = true;
        } catch (Throwable error) {
          errorHandler.handle(log, error);
        }
      }
    }
  }

  private boolean awaitPreprocessing(IMonitorLog log, Future<?> preprocessingResult) {
    boolean interrupted = false;
    try {
      while (true) {
        try {
          preprocessingResult.get();
          return true;
        } catch (InterruptedException e) {
          interrupted = true;
          accepting.set(false);
        } catch (ExecutionException e) {
          errorHandler.handle(log, e.getCause());
          return false;
        }
      }
    } finally {
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }

  private void flushBatch() {
    if (hasUncommittedWrites) {
      writeStrategy.commit();
      hasUncommittedWrites = false;
    }
  }

  private int fixedBatchSize() {
    return ((BatchPolicy.FixedSize) batchPolicy).size();
  }

  private static long toTimeoutNanos(FlushPolicy flushPolicy) {
    Objects.requireNonNull(flushPolicy, "flushPolicy must not be null");
    if (flushPolicy instanceof FlushPolicy.After after) {
      return after.timeout().toNanos();
    }
    return 0L;
  }

  private static BatchPolicy legacyBatchPolicy(int batchSize) {
    return batchSize < 0 ? BatchPolicy.unbounded() : BatchPolicy.fixedSize(batchSize);
  }

  private static FlushPolicy legacyFlushPolicy(Duration flushTimeout) {
    Objects.requireNonNull(flushTimeout, "flushTimeout must not be null");
    return flushTimeout.isZero() ? FlushPolicy.disabled() : FlushPolicy.after(flushTimeout);
  }

  private static ThreadFactory newPreprocessingThreadFactory() {
    int poolNumber = POOL_SEQUENCE.incrementAndGet();
    AtomicInteger threadSequence = new AtomicInteger();
    return task -> {
      Thread thread =
          new Thread(
              task,
              "moniq-preprocessor-" + poolNumber + "-" + threadSequence.incrementAndGet());
      thread.setDaemon(true);
      return thread;
    };
  }
}
