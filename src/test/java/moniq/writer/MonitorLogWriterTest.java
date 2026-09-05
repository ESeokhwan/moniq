package moniq.writer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
    writer.syncedNotify();
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
    writer.syncedNotify();
    writerThread.join(Duration.ofSeconds(2).toMillis());

    assertFalse(writerThread.isAlive());
    assertEquals(List.of("preprocessed"), strategy.writtenIds);
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
