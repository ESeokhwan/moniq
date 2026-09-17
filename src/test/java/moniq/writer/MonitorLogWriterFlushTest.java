package moniq.writer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import moniq.IMonitorLog;
import moniq.MonitorLog;
import moniq.MonitorQueue;
import moniq.writer.strategy.FileMonitorLogWriteStrategy;
import moniq.writer.strategy.IMonitorLogWriteStrategy;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class MonitorLogWriterFlushTest {

  @Test
  void flushesAnIncompleteBatchSynchronously() throws Exception {
    RecordingStrategy strategy = new RecordingStrategy(true);
    RunningWriter running = startWriter(strategy, BatchPolicy.fixedSize(10), FlushPolicy.disabled());

    try {
      running.writer.submit(log("forced"));

      assertTrue(running.writer.flush());
      assertEquals(List.of("forced"), strategy.writtenIds);
      assertEquals(1, strategy.commitCount.get());
      assertTrue(running.queue.isEmpty());
    } finally {
      running.stop();
    }
  }

  @Test
  void emptyFlushSucceedsWithoutCommitting() throws Exception {
    RecordingStrategy strategy = new RecordingStrategy(true);
    RunningWriter running = startWriter(strategy, BatchPolicy.fixedSize(10), FlushPolicy.disabled());

    try {
      assertTrue(running.writer.flush());
      assertEquals(0, strategy.commitCount.get());
    } finally {
      running.stop();
    }
  }

  @Test
  void returnsTheStrategyCommitResult() throws Exception {
    RecordingStrategy strategy = new RecordingStrategy(false);
    RunningWriter running = startWriter(strategy, BatchPolicy.fixedSize(10), FlushPolicy.disabled());

    try {
      running.writer.submit(log("failed-commit"));
      assertFalse(running.writer.flush());
      assertEquals(1, strategy.commitCount.get());
    } finally {
      running.stop();
    }
  }

  @Test
  void returnsFailureFromABatchThatWasAlreadyProcessingBeforeTheBoundary() throws Exception {
    RecordingStrategy strategy = new RecordingStrategy(false);
    RunningWriter running = startWriter(strategy, BatchPolicy.fixedSize(1), FlushPolicy.disabled());
    CountDownLatch preprocessingStarted = new CountDownLatch(1);
    CountDownLatch releasePreprocessing = new CountDownLatch(1);
    ExecutorService caller = Executors.newSingleThreadExecutor();

    try {
      running.writer.submit(
          new BlockingLog("before", preprocessingStarted, releasePreprocessing));
      assertTrue(preprocessingStarted.await(2, TimeUnit.SECONDS));

      Future<Boolean> flushResult = caller.submit(running.writer::flush);
      assertTrue(running.awaitQueueSize(1, 2, TimeUnit.SECONDS));
      releasePreprocessing.countDown();

      assertFalse(flushResult.get(2, TimeUnit.SECONDS));
      assertEquals(List.of("before"), strategy.writtenIds);
      assertEquals(1, strategy.commitCount.get());
    } finally {
      releasePreprocessing.countDown();
      caller.shutdownNow();
      running.stop();
    }
  }

  @Test
  void startsANewBatchAfterForcedFlush() throws Exception {
    RecordingStrategy strategy = new RecordingStrategy(true);
    RunningWriter running = startWriter(strategy, BatchPolicy.fixedSize(2), FlushPolicy.disabled());

    try {
      running.writer.submit(log("first"));
      assertTrue(running.writer.flush());

      running.writer.submit(log("second"));
      assertFalse(strategy.awaitCommitCount(2, 100, TimeUnit.MILLISECONDS));
      running.writer.submit(log("third"));

      assertTrue(strategy.awaitCommitCount(2, 2, TimeUnit.SECONDS));
      assertEquals(List.of("first", "second", "third"), strategy.writtenIds);
    } finally {
      running.stop();
    }
  }

  @Test
  void startsANewTimeoutAfterForcedFlush() throws Exception {
    RecordingStrategy strategy = new RecordingStrategy(true);
    RunningWriter running =
        startWriter(
            strategy,
            BatchPolicy.fixedSize(10),
            FlushPolicy.after(Duration.ofMillis(300)));

    try {
      running.writer.submit(log("first"));
      assertTrue(running.writer.flush());

      running.writer.submit(log("second"));
      assertFalse(strategy.awaitCommitCount(2, 100, TimeUnit.MILLISECONDS));
      assertTrue(strategy.awaitCommitCount(2, 2, TimeUnit.SECONDS));
      assertEquals(List.of("first", "second"), strategy.writtenIds);
    } finally {
      running.stop();
    }
  }

  @Test
  void waitsForPreprocessingAndCommit() throws Exception {
    RecordingStrategy strategy = new RecordingStrategy(true);
    RunningWriter running = startWriter(strategy, BatchPolicy.fixedSize(10), FlushPolicy.disabled());
    CountDownLatch preprocessingStarted = new CountDownLatch(1);
    CountDownLatch releasePreprocessing = new CountDownLatch(1);
    ExecutorService caller = Executors.newSingleThreadExecutor();

    try {
      running.writer.submit(
          new BlockingLog("blocking", preprocessingStarted, releasePreprocessing));
      Future<Boolean> result = caller.submit(running.writer::flush);

      assertTrue(preprocessingStarted.await(2, TimeUnit.SECONDS));
      assertFalse(result.isDone());
      releasePreprocessing.countDown();

      assertTrue(result.get(2, TimeUnit.SECONDS));
      assertEquals(List.of("blocking"), strategy.writtenIds);
      assertEquals(1, strategy.commitCount.get());
    } finally {
      releasePreprocessing.countDown();
      caller.shutdownNow();
      running.stop();
    }
  }

  @Test
  void leavesLogsSubmittedAfterTheFlushBoundaryForTheNextBatch() throws Exception {
    RecordingStrategy strategy = new RecordingStrategy(true);
    RunningWriter running = startWriter(strategy, BatchPolicy.fixedSize(10), FlushPolicy.disabled());
    CountDownLatch preprocessingStarted = new CountDownLatch(1);
    CountDownLatch releasePreprocessing = new CountDownLatch(1);
    ExecutorService caller = Executors.newSingleThreadExecutor();

    try {
      running.writer.submit(
          new BlockingLog("before", preprocessingStarted, releasePreprocessing));
      Future<Boolean> firstFlush = caller.submit(running.writer::flush);

      // The writer dequeues the marker before it starts preprocessing the preceding record.
      assertTrue(preprocessingStarted.await(2, TimeUnit.SECONDS));
      running.writer.submit(log("after"));
      releasePreprocessing.countDown();

      assertTrue(firstFlush.get(2, TimeUnit.SECONDS));
      assertEquals(List.of("before"), strategy.writtenIds);
      assertEquals(1, strategy.commitCount.get());
      assertEquals(1, running.queue.size());

      assertTrue(running.writer.flush());
      assertEquals(List.of("before", "after"), strategy.writtenIds);
      assertEquals(2, strategy.commitCount.get());
    } finally {
      releasePreprocessing.countDown();
      caller.shutdownNow();
      running.stop();
    }
  }

  @Test
  void preservesEachFlushBoundaryInFifoOrder() throws Exception {
    CountDownLatch secondWriteStarted = new CountDownLatch(1);
    CountDownLatch releaseSecondWrite = new CountDownLatch(1);
    BlockingSecondWriteStrategy strategy =
        new BlockingSecondWriteStrategy(secondWriteStarted, releaseSecondWrite);
    RunningWriter running = startWriter(strategy, BatchPolicy.fixedSize(10), FlushPolicy.disabled());
    CountDownLatch preprocessingStarted = new CountDownLatch(1);
    CountDownLatch releasePreprocessing = new CountDownLatch(1);
    ExecutorService callers = Executors.newFixedThreadPool(2);

    try {
      running.writer.submit(
          new BlockingLog("first", preprocessingStarted, releasePreprocessing));
      Future<Boolean> firstFlush = callers.submit(running.writer::flush);
      assertTrue(preprocessingStarted.await(2, TimeUnit.SECONDS));
      running.writer.submit(log("second"));
      Future<Boolean> secondFlush = callers.submit(running.writer::flush);

      releasePreprocessing.countDown();
      assertTrue(secondWriteStarted.await(2, TimeUnit.SECONDS));
      assertTrue(firstFlush.get(2, TimeUnit.SECONDS));
      assertEquals(List.of("first"), strategy.writtenIds);
      assertEquals(1, strategy.commitCount.get());

      releaseSecondWrite.countDown();
      assertTrue(secondFlush.get(2, TimeUnit.SECONDS));
      assertEquals(List.of("first", "second"), strategy.writtenIds);
      assertEquals(2, strategy.commitCount.get());
    } finally {
      releasePreprocessing.countDown();
      releaseSecondWrite.countDown();
      callers.shutdownNow();
      running.stop();
    }
  }

  @Test
  void completesConcurrentFlushRequestsWithoutLosingCallers() throws Exception {
    RecordingStrategy strategy = new RecordingStrategy(true);
    RunningWriter running = startWriter(strategy, BatchPolicy.fixedSize(100), FlushPolicy.disabled());
    ExecutorService callers = Executors.newFixedThreadPool(8);
    CountDownLatch start = new CountDownLatch(1);

    try {
      running.writer.submit(log("one"));
      List<Future<Boolean>> results = new ArrayList<>();
      for (int i = 0; i < 8; i++) {
        results.add(
            callers.submit(
                () -> {
                  start.await();
                  return running.writer.flush();
                }));
      }
      start.countDown();

      for (Future<Boolean> result : results) {
        assertTrue(result.get(2, TimeUnit.SECONDS));
      }
      assertEquals(List.of("one"), strategy.writtenIds);
      assertEquals(1, strategy.commitCount.get());
    } finally {
      callers.shutdownNow();
      running.stop();
    }
  }

  @Test
  void propagatesWriterFailureToFlushCaller() throws Exception {
    RecordingStrategy strategy = new RecordingStrategy(true);
    RunningWriter running = startWriter(strategy, BatchPolicy.fixedSize(10), FlushPolicy.disabled());

    running.writer.submit(new FailingLog());
    IllegalStateException error = assertThrows(IllegalStateException.class, running.writer::flush);
    assertEquals("preprocessing failed", error.getMessage());
    running.thread.join(Duration.ofSeconds(2).toMillis());
    assertFalse(running.thread.isAlive());
    assertThrows(IllegalStateException.class, running.writer::flush);
  }

  @Test
  void rejectsFlushAfterShutdownStarts() throws Exception {
    RunningWriter running =
        startWriter(new RecordingStrategy(true), BatchPolicy.fixedSize(10), FlushPolicy.disabled());

    running.writer.gracefulShutdown();

    assertThrows(IllegalStateException.class, running.writer::flush);
    running.thread.join(Duration.ofSeconds(2).toMillis());
    assertFalse(running.thread.isAlive());
  }

  @Test
  void flushAndRunExecutesTheActionOnTheWriterThreadAfterACommitAttempt() throws Exception {
    RecordingStrategy strategy = new RecordingStrategy(false);
    RunningWriter running = startWriter(strategy, BatchPolicy.fixedSize(10), FlushPolicy.disabled());
    AtomicBoolean actionRan = new AtomicBoolean();
    AtomicReference<Thread> actionThread = new AtomicReference<>();

    try {
      running.writer.submit(log("before"));

      assertFalse(
          running.writer.flushAndRun(
              () -> {
                actionRan.set(true);
                actionThread.set(Thread.currentThread());
              }));

      assertTrue(actionRan.get());
      assertEquals(running.thread, actionThread.get());
      assertEquals(1, strategy.commitCount.get());
    } finally {
      running.stop();
    }
  }

  @Test
  void keepsWritingAfterAFlushAndRunActionFails() throws Exception {
    RecordingStrategy strategy = new RecordingStrategy(true);
    RunningWriter running = startWriter(strategy, BatchPolicy.fixedSize(10), FlushPolicy.disabled());

    try {
      IllegalStateException error =
          assertThrows(
              IllegalStateException.class,
              () ->
                  running.writer.flushAndRun(
                      () -> {
                        throw new IllegalStateException("roll-out failed");
                      }));
      assertEquals("roll-out failed", error.getMessage());

      running.writer.submit(log("after-failure"));
      assertTrue(running.writer.flush());
      assertEquals(List.of("after-failure"), strategy.writtenIds);
    } finally {
      running.stop();
    }
  }

  @Test
  void flushAndRunRollsOutFilesBetweenItsBoundaries(@TempDir Path tempDir) throws Exception {
    Path output = tempDir.resolve("monitor.log");
    FileMonitorLogWriteStrategy strategy =
        new FileMonitorLogWriteStrategy(
            output, FileMonitorLogWriteStrategy.Format.COMMA_SEPARATED);
    RunningWriter running = startWriter(strategy, BatchPolicy.fixedSize(100), FlushPolicy.disabled());

    try {
      running.writer.submit(log("before"));
      assertTrue(running.writer.flushAndRun(strategy.rollOutAction()));
      running.writer.submit(log("after"));
      assertTrue(running.writer.flush());
    } finally {
      running.stop();
      strategy.close();
    }

    assertEquals(
        List.of("RequestType,Id,Timestamp,TimestampNano,State", "type,before,1,10,state"),
        Files.readAllLines(output));
    assertEquals(
        List.of("RequestType,Id,Timestamp,TimestampNano,State", "type,after,1,10,state"),
        Files.readAllLines(tempDir.resolve("monitor.1.log")));
  }

  @Test
  void forcedFlushDoesNotResetFileRollOutCount(@TempDir Path tempDir) throws Exception {
    Path output = tempDir.resolve("monitor.log");
    FileMonitorLogWriteStrategy strategy =
        new FileMonitorLogWriteStrategy(
            output,
            Duration.ofHours(1),
            2,
            FileMonitorLogWriteStrategy.Format.COMMA_SEPARATED);
    RunningWriter running = startWriter(strategy, BatchPolicy.fixedSize(100), FlushPolicy.disabled());

    try {
      running.writer.submit(log("first"));
      assertTrue(running.writer.flush());
      running.writer.submit(log("second"));
      assertTrue(running.writer.flush());
      running.writer.submit(log("third"));
      assertTrue(running.writer.flush());
    } finally {
      running.stop();
      strategy.close();
    }

    assertEquals(
        List.of(
            "RequestType,Id,Timestamp,TimestampNano,State",
            "type,first,1,10,state",
            "type,second,1,10,state"),
        Files.readAllLines(output));
    assertEquals(
        List.of(
            "RequestType,Id,Timestamp,TimestampNano,State",
            "type,third,1,10,state"),
        Files.readAllLines(tempDir.resolve("monitor.1.log")));
  }

  private static RunningWriter startWriter(
      IMonitorLogWriteStrategy strategy, BatchPolicy batchPolicy, FlushPolicy flushPolicy) {
    MonitorQueue queue = new MonitorQueue();
    MonitorLogWriter writer = new MonitorLogWriter(queue, strategy, batchPolicy, flushPolicy);
    Thread thread = new Thread(writer, "monitor-log-writer-flush-test");
    thread.start();
    return new RunningWriter(queue, writer, thread);
  }

  private static MonitorLog log(String id) {
    return new MonitorLog("type", id, "state", 1L, 10L);
  }

  private static final class RunningWriter {
    private final MonitorQueue queue;
    private final MonitorLogWriter writer;
    private final Thread thread;

    private RunningWriter(MonitorQueue queue, MonitorLogWriter writer, Thread thread) {
      this.queue = queue;
      this.writer = writer;
      this.thread = thread;
    }

    private void stop() throws InterruptedException {
      writer.gracefulShutdown();
      thread.join(Duration.ofSeconds(2).toMillis());
      assertFalse(thread.isAlive());
    }

    private boolean awaitQueueSize(int expected, long timeout, TimeUnit unit)
        throws InterruptedException {
      long deadline = System.nanoTime() + unit.toNanos(timeout);
      while (queue.size() != expected) {
        if (System.nanoTime() >= deadline) {
          return false;
        }
        TimeUnit.MILLISECONDS.sleep(1);
      }
      return true;
    }
  }

  private static class RecordingStrategy implements IMonitorLogWriteStrategy {
    protected final List<String> writtenIds = Collections.synchronizedList(new ArrayList<>());
    protected final AtomicInteger commitCount = new AtomicInteger();
    private final boolean commitResult;
    private final Object commitMonitor = new Object();

    private RecordingStrategy(boolean commitResult) {
      this.commitResult = commitResult;
    }

    @Override
    public void write(IMonitorLog log) {
      writtenIds.add(log.getValues().get(1));
    }

    @Override
    public boolean commit() {
      commitCount.incrementAndGet();
      synchronized (commitMonitor) {
        commitMonitor.notifyAll();
      }
      return commitResult;
    }

    private boolean awaitCommitCount(int expected, long timeout, TimeUnit unit)
        throws InterruptedException {
      long deadline = System.nanoTime() + unit.toNanos(timeout);
      synchronized (commitMonitor) {
        while (commitCount.get() < expected) {
          long remaining = deadline - System.nanoTime();
          if (remaining <= 0) {
            return false;
          }
          TimeUnit.NANOSECONDS.timedWait(commitMonitor, remaining);
        }
      }
      return true;
    }
  }

  private static final class BlockingSecondWriteStrategy extends RecordingStrategy {
    private final CountDownLatch secondWriteStarted;
    private final CountDownLatch releaseSecondWrite;

    private BlockingSecondWriteStrategy(
        CountDownLatch secondWriteStarted, CountDownLatch releaseSecondWrite) {
      super(true);
      this.secondWriteStarted = secondWriteStarted;
      this.releaseSecondWrite = releaseSecondWrite;
    }

    @Override
    public void write(IMonitorLog log) {
      if ("second".equals(log.getValues().get(1))) {
        secondWriteStarted.countDown();
        try {
          releaseSecondWrite.await();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new RuntimeException(e);
        }
      }
      super.write(log);
    }
  }

  private static final class BlockingLog implements IMonitorLog {
    private final String id;
    private final CountDownLatch started;
    private final CountDownLatch release;

    private BlockingLog(String id, CountDownLatch started, CountDownLatch release) {
      this.id = id;
      this.started = started;
      this.release = release;
    }

    @Override
    public void preprocess() {
      started.countDown();
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

  private static final class FailingLog implements IMonitorLog {
    @Override
    public void preprocess() {
      throw new IllegalStateException("preprocessing failed");
    }

    @Override
    public List<String> getHeaders() {
      return List.of();
    }

    @Override
    public List<String> getValues() {
      return List.of();
    }
  }
}
