package moniq.writer;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import moniq.IMonitorLog;
import moniq.MonitorQueue;
import moniq.writer.strategy.IMonitorLogWriteStrategy;

public class MonitorLogWriter implements Runnable {

  private final MonitorQueue monitorQueue;
  private final IMonitorLogWriteStrategy writeStrategy;
  private final int batchSize;

  private final AtomicBoolean shutdownRequested = new AtomicBoolean();
  private final AtomicBoolean started = new AtomicBoolean();
  private final ReentrantLock lifecycleLock = new ReentrantLock();
  private final Condition workAvailable = lifecycleLock.newCondition();

  private int currentWrittenCount;

  public MonitorLogWriter(
      MonitorQueue monitorQueue, IMonitorLogWriteStrategy writeStrategy, int batchSize) {
    this.monitorQueue = Objects.requireNonNull(monitorQueue, "monitorQueue must not be null");
    this.writeStrategy = Objects.requireNonNull(writeStrategy, "writeStrategy must not be null");
    if (batchSize == 0) {
      throw new IllegalArgumentException("batchSize must not be zero");
    }
    this.batchSize = batchSize;
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
      if (isBatchReady()) {
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

  /** Signals the writer if the externally managed queue contains a complete batch. */
  public void notifyIfNeeded() {
    lifecycleLock.lock();
    try {
      if (isBatchReady()) {
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
        processLogs(batchSize);
      }
    } finally {
      flushBatch();
    }
  }

  private void awaitBatchOrShutdown() {
    lifecycleLock.lock();
    try {
      while (!shutdownRequested.get() && !isBatchReady()) {
        try {
          workAvailable.await();
        } catch (InterruptedException e) {
          shutdownRequested.set(true);
          Thread.currentThread().interrupt();
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

  private void drainQueue() {
    while (!monitorQueue.isEmpty()) {
      processLogs(batchSize > 0 ? batchSize : Integer.MAX_VALUE);
    }
  }

  private void processLogs(int maximumCount) {
    int processedCount = 0;
    while (processedCount < maximumCount) {
      IMonitorLog log = monitorQueue.dequeue();
      if (log == null) {
        return;
      }
      log.preprocess();
      writeStrategy.write(log);
      currentWrittenCount++;
      processedCount++;
      tryFlushBatch();
    }
  }

  private void tryFlushBatch() {
    if (batchSize > 0 && currentWrittenCount >= batchSize) {
      flushBatch();
    }
  }

  private void flushBatch() {
    writeStrategy.commit();
    currentWrittenCount = 0;
  }
}
