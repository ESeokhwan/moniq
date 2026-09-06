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
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import moniq.IMonitorLog;
import moniq.MonitorQueue;
import moniq.writer.strategy.IMonitorLogWriteStrategy;

/**
 * A one-shot queue consumer that preprocesses logs and writes them in FIFO batches.
 *
 * <p>Use {@link #submit(IMonitorLog)} for coordinated enqueueing. Call {@link
 * #gracefulShutdown()} and join the owning thread to drain and close the writer lifecycle.
 */
public class MonitorLogWriter implements Runnable {

  private static final AtomicInteger POOL_SEQUENCE = new AtomicInteger();

  private final MonitorQueue monitorQueue;
  private final IMonitorLogWriteStrategy writeStrategy;
  private final int batchSize;
  private final long flushTimeoutNanos;
  private final MonitorLogErrorHandler errorHandler;
  private final ExecutorService preprocessingExecutor;

  private final AtomicBoolean shutdownRequested = new AtomicBoolean();
  private final AtomicBoolean started = new AtomicBoolean();
  private final ReentrantLock lifecycleLock = new ReentrantLock();
  private final Condition workAvailable = lifecycleLock.newCondition();

  private long pendingSinceNanos = Long.MIN_VALUE;
  private boolean hasUncommittedWrites;

  public MonitorLogWriter(
      MonitorQueue monitorQueue, IMonitorLogWriteStrategy writeStrategy, int batchSize) {
    this(monitorQueue, writeStrategy, batchSize, Duration.ZERO);
  }

  public MonitorLogWriter(
      MonitorQueue monitorQueue,
      IMonitorLogWriteStrategy writeStrategy,
      int batchSize,
      Duration flushTimeout) {
    this(monitorQueue, writeStrategy, batchSize, flushTimeout, 1);
  }

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

  public MonitorLogWriter(
      MonitorQueue monitorQueue,
      IMonitorLogWriteStrategy writeStrategy,
      int batchSize,
      Duration flushTimeout,
      int workerCount,
      MonitorLogErrorHandler errorHandler) {
    this.monitorQueue = Objects.requireNonNull(monitorQueue, "monitorQueue must not be null");
    this.writeStrategy = Objects.requireNonNull(writeStrategy, "writeStrategy must not be null");
    if (batchSize == 0) {
      throw new IllegalArgumentException("batchSize must not be zero");
    }
    this.batchSize = batchSize;
    this.flushTimeoutNanos = toTimeoutNanos(flushTimeout);
    if (workerCount <= 0) {
      throw new IllegalArgumentException("workerCount must be greater than zero");
    }
    this.errorHandler = Objects.requireNonNull(errorHandler, "errorHandler must not be null");
    this.preprocessingExecutor =
        Executors.newFixedThreadPool(workerCount, newPreprocessingThreadFactory());
  }

  /**
   * Enqueues a log and wakes the writer when a complete batch is ready.
   *
   * @throws IllegalStateException if shutdown has already been requested
   */
  public void submit(IMonitorLog log) {
    Objects.requireNonNull(log, "log must not be null");
    lifecycleLock.lock();
    try {
      if (shutdownRequested.get()) {
        throw new IllegalStateException("MonitorLogWriter is shutting down.");
      }
      monitorQueue.enqueue(log);
      markPendingIfNeeded();
      if (shouldSignalForCurrentQueue()) {
        workAvailable.signal();
      }
    } finally {
      lifecycleLock.unlock();
    }
  }

  public void gracefulShutdown() {
    lifecycleLock.lock();
    try {
      shutdownRequested.set(true);
      workAvailable.signalAll();
    } finally {
      lifecycleLock.unlock();
    }
  }

  /** Waits for a signal. Prefer {@link #submit(IMonitorLog)} for normal producer usage. */
  public void syncedWait() {
    lifecycleLock.lock();
    try {
      workAvailable.await();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } finally {
      lifecycleLock.unlock();
    }
  }

  public void syncedNotify() {
    lifecycleLock.lock();
    try {
      workAvailable.signalAll();
    } finally {
      lifecycleLock.unlock();
    }
  }

  /** Signals the writer if the externally managed queue is ready or needs a timeout scheduled. */
  public void notifyIfNeeded() {
    lifecycleLock.lock();
    try {
      markPendingIfNeeded();
      if (shouldSignalForCurrentQueue()) {
        workAvailable.signal();
      }
    } finally {
      lifecycleLock.unlock();
    }
  }

  @Override
  public void run() {
    if (!started.compareAndSet(false, true)) {
      throw new IllegalStateException("MonitorLogWriter can only be run once.");
    }

    try {
      while (true) {
        awaitBatchOrShutdown();
        if (shutdownRequested.get()) {
          drainQueue();
          return;
        }
        boolean completeBatchReady = isBatchReady();
        int maximumCount = completeBatchReady ? batchSize : monitorQueue.size();
        processLogs(maximumCount);
        flushBatch();
        resetPendingTimer();
      }
    } finally {
      preprocessingExecutor.shutdownNow();
      flushBatch();
    }
  }

  private void awaitBatchOrShutdown() {
    lifecycleLock.lock();
    try {
      while (!shutdownRequested.get()) {
        if (isBatchReady()) {
          return;
        }

        if (monitorQueue.isEmpty()) {
          pendingSinceNanos = Long.MIN_VALUE;
        } else if (flushTimeoutNanos > 0) {
          markPendingIfNeeded();
          long elapsedNanos = System.nanoTime() - pendingSinceNanos;
          long remainingNanos = flushTimeoutNanos - elapsedNanos;
          if (remainingNanos <= 0) {
            return;
          }
          try {
            workAvailable.awaitNanos(remainingNanos);
          } catch (InterruptedException e) {
            requestShutdownAfterInterrupt();
            return;
          }
          continue;
        }

        try {
          workAvailable.await();
        } catch (InterruptedException e) {
          requestShutdownAfterInterrupt();
          return;
        }
      }
    } finally {
      lifecycleLock.unlock();
    }
  }

  private boolean isBatchReady() {
    return batchSize > 0 && monitorQueue.size() >= batchSize;
  }

  private boolean shouldSignalForCurrentQueue() {
    return isBatchReady() || (flushTimeoutNanos > 0 && !monitorQueue.isEmpty());
  }

  private void markPendingIfNeeded() {
    if (flushTimeoutNanos > 0
        && pendingSinceNanos == Long.MIN_VALUE
        && !monitorQueue.isEmpty()) {
      pendingSinceNanos = System.nanoTime();
    }
  }

  private void resetPendingTimer() {
    lifecycleLock.lock();
    try {
      pendingSinceNanos = monitorQueue.isEmpty() ? Long.MIN_VALUE : System.nanoTime();
    } finally {
      lifecycleLock.unlock();
    }
  }

  private void requestShutdownAfterInterrupt() {
    shutdownRequested.set(true);
    Thread.currentThread().interrupt();
  }

  private void drainQueue() {
    while (!monitorQueue.isEmpty()) {
      processLogs(batchSize > 0 ? batchSize : Integer.MAX_VALUE);
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
          shutdownRequested.set(true);
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

  private static long toTimeoutNanos(Duration flushTimeout) {
    Objects.requireNonNull(flushTimeout, "flushTimeout must not be null");
    if (flushTimeout.isNegative()) {
      throw new IllegalArgumentException("flushTimeout must not be negative");
    }
    try {
      return flushTimeout.toNanos();
    } catch (ArithmeticException e) {
      throw new IllegalArgumentException("flushTimeout is too large", e);
    }
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
