package moniq.writer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import moniq.IMonitorLog;
import moniq.MonitorLog;
import moniq.MonitorQueue;
import moniq.exception.NotProcessedException;
import moniq.writer.strategy.IMonitorLogWriteStrategy;
import org.junit.jupiter.api.Test;

class MonitorLogWriterTest {

  @Test
  void writesAndCommitsACompleteBatch() throws Exception {
    MonitorQueue queue = new MonitorQueue();
    RecordingWriteStrategy strategy = new RecordingWriteStrategy();
    MonitorLogWriter writer = new MonitorLogWriter(queue, strategy, 2);
    queue.enqueue(new MonitorLog("type", "first", "state", 1L, 10L));
    queue.enqueue(new MonitorLog("type", "second", "state", 2L, 20L));

    Thread writerThread = new Thread(writer);
    writerThread.start();

    assertTrue(strategy.committed.await(2, TimeUnit.SECONDS));
    writer.gracefulShutdown();
    writerThread.join(Duration.ofSeconds(2).toMillis());

    assertFalse(writerThread.isAlive());
    assertEquals(List.of("first", "second"), strategy.writtenIds);
    assertEquals(1, strategy.commitCount);
  }

  @Test
  void preprocessesEachLogBeforeWritingIt() throws Exception {
    MonitorQueue queue = new MonitorQueue();
    RecordingWriteStrategy strategy = new RecordingWriteStrategy();
    MonitorLogWriter writer = new MonitorLogWriter(queue, strategy, 1);
    queue.enqueue(new PreprocessingMonitorLog("preprocessed"));

    Thread writerThread = new Thread(writer);
    writerThread.start();

    assertTrue(strategy.committed.await(2, TimeUnit.SECONDS));
    writer.gracefulShutdown();
    writerThread.join(Duration.ofSeconds(2).toMillis());

    assertFalse(writerThread.isAlive());
    assertEquals(List.of("preprocessed"), strategy.writtenIds);
  }

  @Test
  void shutdownWakesWriterAndFlushesAnIncompleteBatch() throws Exception {
    MonitorQueue queue = new MonitorQueue();
    RecordingWriteStrategy strategy = new RecordingWriteStrategy();
    MonitorLogWriter writer = new MonitorLogWriter(queue, strategy, 2);
    Thread writerThread = new Thread(writer);
    writerThread.start();

    writer.submit(new MonitorLog("type", "partial", "state", 1L, 10L));
    assertFalse(strategy.committed.await(100, TimeUnit.MILLISECONDS));

    writer.gracefulShutdown();
    writerThread.join(Duration.ofSeconds(2).toMillis());

    assertFalse(writerThread.isAlive());
    assertEquals(List.of("partial"), strategy.writtenIds);
    assertEquals(1, strategy.commitCount);
  }

  @Test
  void submitWakesWriterWhenBatchBecomesReady() throws Exception {
    MonitorQueue queue = new MonitorQueue();
    RecordingWriteStrategy strategy = new RecordingWriteStrategy();
    MonitorLogWriter writer = new MonitorLogWriter(queue, strategy, 1);
    Thread writerThread = new Thread(writer);
    writerThread.start();

    writer.submit(new MonitorLog("type", "submitted", "state", 1L, 10L));

    assertTrue(strategy.committed.await(2, TimeUnit.SECONDS));
    writer.gracefulShutdown();
    writerThread.join(Duration.ofSeconds(2).toMillis());
    assertEquals(List.of("submitted"), strategy.writtenIds);
  }

  @Test
  void rejectsSubmissionAfterShutdown() {
    MonitorLogWriter writer =
        new MonitorLogWriter(new MonitorQueue(), new RecordingWriteStrategy(), 1);

    writer.gracefulShutdown();

    assertThrows(
        IllegalStateException.class,
        () -> writer.submit(new MonitorLog("type", "late", "state", 1L, 10L)));
  }

  @Test
  void emptyShutdownDoesNotCommit() {
    RecordingWriteStrategy strategy = new RecordingWriteStrategy();
    MonitorLogWriter writer = new MonitorLogWriter(new MonitorQueue(), strategy, 1);

    writer.gracefulShutdown();
    writer.run();

    assertEquals(0, strategy.commitCount);
  }

  @Test
  void rejectsInvalidConstructionAndMultipleRuns() {
    assertThrows(
        NullPointerException.class,
        () -> new MonitorLogWriter(null, new RecordingWriteStrategy(), 1));
    assertThrows(
        NullPointerException.class,
        () -> new MonitorLogWriter(new MonitorQueue(), null, 1));
    assertThrows(
        IllegalArgumentException.class,
        () -> new MonitorLogWriter(new MonitorQueue(), new RecordingWriteStrategy(), 0));
    assertThrows(
        NullPointerException.class,
        () ->
            new MonitorLogWriter(
                new MonitorQueue(), new RecordingWriteStrategy(), 1, null));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new MonitorLogWriter(
                new MonitorQueue(),
                new RecordingWriteStrategy(),
                1,
                Duration.ofMillis(-1)));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new MonitorLogWriter(
                new MonitorQueue(),
                new RecordingWriteStrategy(),
                1,
                Duration.ZERO,
                0));
    assertThrows(
        NullPointerException.class,
        () ->
            new MonitorLogWriter(
                new MonitorQueue(),
                new RecordingWriteStrategy(),
                1,
                Duration.ZERO,
                1,
                null));

    MonitorLogWriter writer =
        new MonitorLogWriter(new MonitorQueue(), new RecordingWriteStrategy(), 1);
    writer.gracefulShutdown();
    writer.run();
    assertThrows(IllegalStateException.class, writer::run);
  }

  @Test
  void flushesAnIncompleteBatchAfterTimeout() throws Exception {
    MonitorQueue queue = new MonitorQueue();
    RecordingWriteStrategy strategy = new RecordingWriteStrategy();
    MonitorLogWriter writer =
        new MonitorLogWriter(queue, strategy, 10, Duration.ofMillis(50));
    Thread writerThread = new Thread(writer);
    writerThread.start();

    writer.submit(new MonitorLog("type", "timed", "state", 1L, 10L));

    assertTrue(strategy.committed.await(2, TimeUnit.SECONDS));
    assertEquals(List.of("timed"), strategy.writtenIds);
    writer.gracefulShutdown();
    writerThread.join(Duration.ofSeconds(2).toMillis());
    assertFalse(writerThread.isAlive());
  }

  @Test
  void unboundedBatchFlushesAllPendingLogsOnTimeout() throws Exception {
    MonitorQueue queue = new MonitorQueue();
    RecordingWriteStrategy strategy = new RecordingWriteStrategy();
    MonitorLogWriter writer =
        new MonitorLogWriter(queue, strategy, -1, Duration.ofMillis(50));
    Thread writerThread = new Thread(writer);
    writerThread.start();

    writer.submit(new MonitorLog("type", "first", "state", 1L, 10L));
    writer.submit(new MonitorLog("type", "second", "state", 2L, 20L));
    writer.submit(new MonitorLog("type", "third", "state", 3L, 30L));

    assertTrue(strategy.committed.await(2, TimeUnit.SECONDS));
    assertEquals(List.of("first", "second", "third"), strategy.writtenIds);
    writer.gracefulShutdown();
    writerThread.join(Duration.ofSeconds(2).toMillis());
    assertFalse(writerThread.isAlive());
  }

  @Test
  void unboundedBatchWithoutTimeoutWaitsForShutdown() throws Exception {
    MonitorQueue queue = new MonitorQueue();
    RecordingWriteStrategy strategy = new RecordingWriteStrategy();
    MonitorLogWriter writer = new MonitorLogWriter(queue, strategy, -1);
    Thread writerThread = new Thread(writer);
    writerThread.start();

    writer.submit(new MonitorLog("type", "pending", "state", 1L, 10L));
    assertFalse(strategy.committed.await(100, TimeUnit.MILLISECONDS));

    writer.gracefulShutdown();
    writerThread.join(Duration.ofSeconds(2).toMillis());
    assertFalse(writerThread.isAlive());
    assertEquals(List.of("pending"), strategy.writtenIds);
    assertEquals(1, strategy.commitCount);
  }

  @Test
  void preprocessesInParallelButWritesInQueueOrder() throws Exception {
    MonitorQueue queue = new MonitorQueue();
    RecordingWriteStrategy strategy = new RecordingWriteStrategy();
    CountDownLatch releaseFirst = new CountDownLatch(1);
    CountDownLatch secondStarted = new CountDownLatch(1);
    MonitorLogWriter writer =
        new MonitorLogWriter(queue, strategy, 2, Duration.ZERO, 2);
    Thread writerThread = new Thread(writer);
    writerThread.start();

    try {
      writer.submit(new BlockingPreprocessingMonitorLog("first", releaseFirst));
      writer.submit(new SignallingPreprocessingMonitorLog("second", secondStarted));

      assertTrue(secondStarted.await(2, TimeUnit.SECONDS));
      assertTrue(strategy.writtenIds.isEmpty());
      releaseFirst.countDown();
      assertTrue(strategy.committed.await(2, TimeUnit.SECONDS));
      assertEquals(List.of("first", "second"), strategy.writtenIds);
    } finally {
      releaseFirst.countDown();
      writer.gracefulShutdown();
      writerThread.join(Duration.ofSeconds(2).toMillis());
    }
    assertFalse(writerThread.isAlive());
  }

  @Test
  void reusesPreprocessingWorkerAcrossBatches() throws Exception {
    MonitorQueue queue = new MonitorQueue();
    RecordingWriteStrategy strategy = new RecordingWriteStrategy();
    AtomicReference<Thread> firstWorker = new AtomicReference<>();
    AtomicReference<Thread> secondWorker = new AtomicReference<>();
    CountDownLatch firstProcessed = new CountDownLatch(1);
    CountDownLatch secondProcessed = new CountDownLatch(1);
    MonitorLogWriter writer =
        new MonitorLogWriter(queue, strategy, 1, Duration.ZERO, 1);
    Thread writerThread = new Thread(writer);
    writerThread.start();

    writer.submit(new ThreadRecordingMonitorLog("first", firstWorker, firstProcessed));
    assertTrue(firstProcessed.await(2, TimeUnit.SECONDS));
    writer.submit(new ThreadRecordingMonitorLog("second", secondWorker, secondProcessed));
    assertTrue(secondProcessed.await(2, TimeUnit.SECONDS));

    writer.gracefulShutdown();
    writerThread.join(Duration.ofSeconds(2).toMillis());
    assertFalse(writerThread.isAlive());
    assertEquals(firstWorker.get(), secondWorker.get());
    assertEquals(List.of("first", "second"), strategy.writtenIds);
  }

  @Test
  void reportsPreprocessingFailureAndContinuesWhenHandlerReturns() throws Exception {
    MonitorQueue queue = new MonitorQueue();
    RecordingWriteStrategy strategy = new RecordingWriteStrategy();
    AtomicReference<IMonitorLog> failedLog = new AtomicReference<>();
    AtomicReference<Throwable> reportedError = new AtomicReference<>();
    CountDownLatch errorReported = new CountDownLatch(1);
    MonitorLogWriter writer =
        new MonitorLogWriter(
            queue,
            strategy,
            2,
            Duration.ZERO,
            2,
            (log, error) -> {
              failedLog.set(log);
              reportedError.set(error);
              errorReported.countDown();
            });
    FailingPreprocessingMonitorLog failingLog =
        new FailingPreprocessingMonitorLog("failed");
    Thread writerThread = new Thread(writer);
    writerThread.start();

    writer.submit(failingLog);
    writer.submit(new PreprocessingMonitorLog("written"));

    assertTrue(errorReported.await(2, TimeUnit.SECONDS));
    assertTrue(strategy.committed.await(2, TimeUnit.SECONDS));
    writer.gracefulShutdown();
    writerThread.join(Duration.ofSeconds(2).toMillis());

    assertFalse(writerThread.isAlive());
    assertEquals(failingLog, failedLog.get());
    assertEquals("preprocessing failed", reportedError.get().getMessage());
    assertEquals(List.of("written"), strategy.writtenIds);
  }

  private static final class RecordingWriteStrategy implements IMonitorLogWriteStrategy {
    private final List<String> writtenIds = new ArrayList<>();
    private final CountDownLatch committed = new CountDownLatch(1);
    private int commitCount;

    @Override
    public void write(IMonitorLog log) {
      writtenIds.add(log.getValues().get(1));
    }

    @Override
    public boolean commit() {
      commitCount++;
      committed.countDown();
      return true;
    }
  }

  private static final class PreprocessingMonitorLog implements IMonitorLog {
    private final String id;
    private boolean processed;

    private PreprocessingMonitorLog(String id) {
      this.id = id;
    }

    @Override
    public void preprocess() {
      processed = true;
    }

    @Override
    public List<String> getHeaders() {
      return List.of("Type", "Id");
    }

    @Override
    public List<String> getValues() {
      if (!processed) {
        throw new NotProcessedException();
      }
      return List.of("type", id);
    }
  }

  private static final class BlockingPreprocessingMonitorLog implements IMonitorLog {
    private final String id;
    private final CountDownLatch release;

    private BlockingPreprocessingMonitorLog(String id, CountDownLatch release) {
      this.id = id;
      this.release = release;
    }

    @Override
    public void preprocess() {
      try {
        release.await();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
    }

    @Override
    public List<String> getHeaders() {
      return List.of("Type", "Id");
    }

    @Override
    public List<String> getValues() {
      return List.of("type", id);
    }
  }

  private static final class SignallingPreprocessingMonitorLog implements IMonitorLog {
    private final String id;
    private final CountDownLatch started;

    private SignallingPreprocessingMonitorLog(String id, CountDownLatch started) {
      this.id = id;
      this.started = started;
    }

    @Override
    public void preprocess() {
      started.countDown();
    }

    @Override
    public List<String> getHeaders() {
      return List.of("Type", "Id");
    }

    @Override
    public List<String> getValues() {
      return List.of("type", id);
    }
  }

  private static final class ThreadRecordingMonitorLog implements IMonitorLog {
    private final String id;
    private final AtomicReference<Thread> worker;
    private final CountDownLatch processed;

    private ThreadRecordingMonitorLog(
        String id, AtomicReference<Thread> worker, CountDownLatch processed) {
      this.id = id;
      this.worker = worker;
      this.processed = processed;
    }

    @Override
    public void preprocess() {
      worker.set(Thread.currentThread());
      processed.countDown();
    }

    @Override
    public List<String> getHeaders() {
      return List.of("Type", "Id");
    }

    @Override
    public List<String> getValues() {
      return List.of("type", id);
    }
  }

  private static final class FailingPreprocessingMonitorLog implements IMonitorLog {
    private final String id;

    private FailingPreprocessingMonitorLog(String id) {
      this.id = id;
    }

    @Override
    public void preprocess() {
      throw new IllegalStateException("preprocessing failed");
    }

    @Override
    public List<String> getHeaders() {
      return List.of("Type", "Id");
    }

    @Override
    public List<String> getValues() {
      return List.of("type", id);
    }
  }
}
