package moniq.writer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import moniq.IMonitorLog;
import moniq.MonitorLog;
import moniq.MonitorQueue;
import moniq.writer.strategy.IMonitorLogWriteStrategy;
import org.junit.jupiter.api.Test;

class MonitorLogWriterConcurrencyTest {

  @Test
  void drainsSubmissionThatWasInFlightWhenShutdownStarted() throws Exception {
    BlockingEnqueueQueue queue = new BlockingEnqueueQueue();
    IdRecordingStrategy strategy = new IdRecordingStrategy();
    MonitorLogWriter writer =
        new MonitorLogWriter(queue, strategy, BatchPolicy.fixedSize(1));
    Thread writerThread = new Thread(writer);
    writerThread.start();
    AtomicReference<Throwable> producerFailure = new AtomicReference<>();
    Thread producer =
        new Thread(
            () -> {
              try {
                writer.submit(new MonitorLog("type", "accepted", "state", 1L, 10L));
              } catch (Throwable error) {
                producerFailure.set(error);
              }
            });
    producer.start();

    try {
      assertTrue(queue.enqueueEntered.await(2, TimeUnit.SECONDS));
      writer.gracefulShutdown();
      writerThread.join(100);
      assertTrue(writerThread.isAlive());
    } finally {
      queue.allowEnqueue.countDown();
      writer.gracefulShutdown();
      producer.join(Duration.ofSeconds(2).toMillis());
      writerThread.join(Duration.ofSeconds(2).toMillis());
    }

    assertFalse(producer.isAlive());
    assertFalse(writerThread.isAlive());
    assertNull(producerFailure.get());
    assertEquals(Set.of("accepted"), strategy.writtenIds);
  }

  @Test
  void acceptsSubmissionsBeforeWriterThreadStarts() throws Exception {
    MonitorQueue queue = new MonitorQueue();
    IdRecordingStrategy strategy = new IdRecordingStrategy();
    MonitorLogWriter writer =
        new MonitorLogWriter(queue, strategy, BatchPolicy.fixedSize(1));

    writer.submit(new MonitorLog("type", "early", "state", 1L, 10L));
    Thread writerThread = new Thread(writer);
    writerThread.start();

    try {
      assertTrue(strategy.committed.await(2, TimeUnit.SECONDS));
    } finally {
      writer.gracefulShutdown();
      writerThread.join(Duration.ofSeconds(2).toMillis());
    }
    assertFalse(writerThread.isAlive());
    assertEquals(Set.of("early"), strategy.writtenIds);
  }

  @Test
  void drainsConcurrentProducerSubmissionsWithoutLoss() throws Exception {
    int producerCount = 8;
    int logsPerProducer = 500;
    int expectedLogCount = producerCount * logsPerProducer;
    MonitorQueue queue = new MonitorQueue();
    IdRecordingStrategy strategy = new IdRecordingStrategy();
    MonitorLogWriter writer =
        new MonitorLogWriter(queue, strategy, BatchPolicy.fixedSize(64));
    Thread writerThread = new Thread(writer);
    writerThread.start();
    ExecutorService producers = Executors.newFixedThreadPool(producerCount);

    try {
      @SuppressWarnings("unchecked")
      Future<Void>[] results = new Future[producerCount];
      for (int producerIndex = 0; producerIndex < producerCount; producerIndex++) {
        int capturedProducerIndex = producerIndex;
        results[producerIndex] =
            producers.submit(
                () -> {
                  for (int logIndex = 0; logIndex < logsPerProducer; logIndex++) {
                    String id = capturedProducerIndex + "-" + logIndex;
                    writer.submit(new MonitorLog("type", id, "state", 1L, 10L));
                  }
                  return null;
                });
      }
      for (Future<Void> result : results) {
        result.get(5, TimeUnit.SECONDS);
      }
    } finally {
      producers.shutdownNow();
      writer.gracefulShutdown();
      writerThread.join(Duration.ofSeconds(5).toMillis());
    }

    assertFalse(writerThread.isAlive());
    assertEquals(expectedLogCount, strategy.writtenIds.size());
    assertEquals(expectedLogCount, strategy.writeCount);
    assertTrue(queue.isEmpty());
  }

  @Test
  void interruptStopsIdleWriterAndRejectsLaterSubmissions() throws Exception {
    MonitorLogWriter writer =
        new MonitorLogWriter(
            new MonitorQueue(), new IdRecordingStrategy(), BatchPolicy.fixedSize(1));
    Thread writerThread = new Thread(writer);
    writerThread.start();

    writerThread.interrupt();
    writerThread.join(Duration.ofSeconds(2).toMillis());

    assertFalse(writerThread.isAlive());
    assertThrows(
        IllegalStateException.class,
        () -> writer.submit(new MonitorLog("type", "late", "state", 1L, 10L)));
  }

  @Test
  void writerFailureClosesSubmissionPath() throws Exception {
    MonitorLogWriter writer =
        new MonitorLogWriter(
            new MonitorQueue(), new IdRecordingStrategy(), BatchPolicy.fixedSize(1));
    AtomicReference<Throwable> uncaughtFailure = new AtomicReference<>();
    CountDownLatch failed = new CountDownLatch(1);
    Thread writerThread = new Thread(writer);
    writerThread.setUncaughtExceptionHandler(
        (thread, error) -> {
          uncaughtFailure.set(error);
          failed.countDown();
        });
    writerThread.start();

    try {
      writer.submit(new FailingMonitorLog());
      assertTrue(failed.await(2, TimeUnit.SECONDS));
    } finally {
      writer.gracefulShutdown();
      writerThread.join(Duration.ofSeconds(2).toMillis());
    }
    assertEquals("preprocessing failed", uncaughtFailure.get().getMessage());
    assertThrows(
        IllegalStateException.class,
        () -> writer.submit(new MonitorLog("type", "late", "state", 1L, 10L)));
  }

  private static final class BlockingEnqueueQueue extends MonitorQueue {
    private final CountDownLatch enqueueEntered = new CountDownLatch(1);
    private final CountDownLatch allowEnqueue = new CountDownLatch(1);

    @Override
    public boolean enqueue(IMonitorLog log) {
      enqueueEntered.countDown();
      try {
        allowEnqueue.await();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
      return super.enqueue(log);
    }
  }

  private static final class IdRecordingStrategy implements IMonitorLogWriteStrategy {
    private final Set<String> writtenIds = ConcurrentHashMap.newKeySet();
    private final CountDownLatch committed = new CountDownLatch(1);
    private int writeCount;

    @Override
    public void write(IMonitorLog log) {
      writtenIds.add(log.getValues().get(1));
      writeCount++;
    }

    @Override
    public boolean commit() {
      committed.countDown();
      return true;
    }
  }

  private static final class FailingMonitorLog implements IMonitorLog {

    @Override
    public void preprocess() {
      throw new IllegalStateException("preprocessing failed");
    }

    @Override
    public java.util.List<String> getHeaders() {
      return java.util.List.of();
    }

    @Override
    public java.util.List<String> getValues() {
      return java.util.List.of();
    }
  }
}
