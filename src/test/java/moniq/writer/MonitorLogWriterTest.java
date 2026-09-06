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
    assertEquals(2, strategy.commitCount);
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

    MonitorLogWriter writer =
        new MonitorLogWriter(new MonitorQueue(), new RecordingWriteStrategy(), 1);
    writer.gracefulShutdown();
    writer.run();
    assertThrows(IllegalStateException.class, writer::run);
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
}
