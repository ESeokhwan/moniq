package moniq.writer.strategy;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static moniq.writer.strategy.FileMonitorLogWriteStrategy.Format.COMMA_SEPARATED;

import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import moniq.MonitorLog;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class FileMonitorLogWriteStrategyTest {

  private static final String HEADER = "RequestType,Id,Timestamp,TimestampNano,State";

  @TempDir Path tempDir;

  @Test
  void createsDirectoriesAndFlushesCommaSeparatedUtf8LogsWithEscaping() throws Exception {
    Path output = tempDir.resolve("nested/monitor.log");
    try (var strategy = new FileMonitorLogWriteStrategy(output, COMMA_SEPARATED)) {
      strategy.write(new MonitorLog("요청,유형", "id\"1", "first\nsecond\rthird", 1000, 2000));
      assertTrue(strategy.commit());
      assertEquals(HEADER + "\n\"요청,유형\",\"id\"\"1\",1000,2000,\"first\nsecond\rthird\"\n",
          Files.readString(output));
    }
  }

  @Test
  void defaultsToHumanReadableLogsAndRollsOutWithoutHeaders() throws Exception {
    Path output = tempDir.resolve("readable.log");
    try (var strategy = new FileMonitorLogWriteStrategy(output, Duration.ZERO, 1)) {
      strategy.write(log(1));
      strategy.write(log(2));
      strategy.rollOut();
      strategy.write(log(3));
    }
    for (int id = 1; id <= 3; id++) {
      Path file = id == 1 ? output : tempDir.resolve("readable." + (id - 1) + ".log");
      assertEquals("RequestType: produce, Id: message-" + id
          + ", Timestamp: 1000, TimestampNano: 2000, State: requested\n",
          Files.readString(file));
    }
  }

  @Test
  void keepsHumanReadableRecordsOnOneLine() throws Exception {
    Path output = tempDir.resolve("readable.log");
    try (var strategy = new FileMonitorLogWriteStrategy(output)) {
      strategy.write(new MonitorLog("요청", "id\\1", "first\nsecond\rthird", 1000, 2000));
    }
    assertEquals("RequestType: 요청, Id: id\\\\1, Timestamp: 1000, TimestampNano: 2000, "
        + "State: first\\nsecond\\rthird\n", Files.readString(output));
    assertEquals(1, Files.readAllLines(output).size());
  }

  @Test
  void rollsOutBeforeExceedingLogCountAndRepeatsHeaders() throws Exception {
    Path output = tempDir.resolve("monitor.log");
    try (var strategy = new FileMonitorLogWriteStrategy(output, Duration.ZERO, 2, COMMA_SEPARATED)) {
      strategy.write(log(1));
      strategy.write(log(2));
      assertTrue(strategy.commit());
      assertFalse(Files.exists(tempDir.resolve("monitor.1.log")));
      strategy.write(log(3));
      assertEquals(List.of(HEADER, row(1), row(2)), Files.readAllLines(output));
    }
    assertEquals(List.of(HEADER, row(3)), Files.readAllLines(tempDir.resolve("monitor.1.log")));
  }

  @Test
  void rollsOutAtExactTimeBoundaryAndResetsTimer() throws Exception {
    AtomicLong time = new AtomicLong();
    Path output = tempDir.resolve("monitor.log");
    try (var strategy = new FileMonitorLogWriteStrategy(
        output, Duration.ofSeconds(10), 0, COMMA_SEPARATED, time::get)) {
      // The interval starts with the first write, not construction.
      time.set(Duration.ofSeconds(100).toNanos());
      strategy.write(log(1));
      time.addAndGet(Duration.ofSeconds(10).toNanos() - 1);
      strategy.write(log(2));
      time.incrementAndGet();
      strategy.write(log(3));
      time.addAndGet(Duration.ofSeconds(10).toNanos());
      strategy.write(log(4));
    }
    assertEquals(List.of(HEADER, row(1), row(2)), Files.readAllLines(output));
    assertEquals(List.of(HEADER, row(3)), Files.readAllLines(tempDir.resolve("monitor.1.log")));
    assertEquals(List.of(HEADER, row(4)), Files.readAllLines(tempDir.resolve("monitor.2.log")));
  }

  @Test
  void eitherLimitCanTriggerRollOut() throws Exception {
    AtomicLong time = new AtomicLong();
    Path output = tempDir.resolve("monitor.log");
    try (var strategy = new FileMonitorLogWriteStrategy(
        output, Duration.ofSeconds(10), 2, COMMA_SEPARATED, time::get)) {
      strategy.write(log(1));
      strategy.write(log(2));
      strategy.write(log(3)); // Count limit.
      time.set(Duration.ofSeconds(10).toNanos());
      strategy.write(log(4)); // Time limit, despite only one log in the previous file.
    }
    assertEquals(List.of(HEADER, row(1), row(2)), Files.readAllLines(output));
    assertEquals(List.of(HEADER, row(3)), Files.readAllLines(tempDir.resolve("monitor.1.log")));
    assertEquals(List.of(HEADER, row(4)), Files.readAllLines(tempDir.resolve("monitor.2.log")));
  }

  @Test
  void manualRollOutFlushesAndResetsBothLimitsWithoutCreatingEmptyFiles() throws Exception {
    AtomicLong time = new AtomicLong();
    Path output = tempDir.resolve("monitor.log");
    try (var strategy = new FileMonitorLogWriteStrategy(
        output, Duration.ofSeconds(10), 2, COMMA_SEPARATED, time::get)) {
      strategy.rollOut();
      assertTrue(strategy.commit());
      assertFalse(Files.exists(output));
      strategy.write(log(1));
      strategy.rollOut();
      strategy.rollOut();
      assertEquals(List.of(HEADER, row(1)), Files.readAllLines(output));
      assertFalse(Files.exists(tempDir.resolve("monitor.1.log")));
      time.set(Duration.ofSeconds(9).toNanos());
      strategy.write(log(2));
      time.set(Duration.ofSeconds(10).toNanos());
      strategy.write(log(3));
    }
    assertEquals(List.of(HEADER, row(2), row(3)), Files.readAllLines(tempDir.resolve("monitor.1.log")));
    assertFalse(Files.exists(tempDir.resolve("monitor.2.log")));
  }

  @Test
  void skipsExistingFilesAcrossStrategyInstances() throws Exception {
    Path output = tempDir.resolve("monitor.log");
    Files.writeString(output, "existing log");
    Files.writeString(tempDir.resolve("monitor.1.log"), "existing rolled log");
    try (var first = new FileMonitorLogWriteStrategy(output, COMMA_SEPARATED);
        var second = new FileMonitorLogWriteStrategy(output, COMMA_SEPARATED)) {
      first.write(log(1));
      second.write(log(2));
      first.rollOut();
      first.write(log(3));
    }
    assertEquals("existing log", Files.readString(output));
    assertEquals("existing rolled log", Files.readString(tempDir.resolve("monitor.1.log")));
    assertEquals(List.of(HEADER, row(1)), Files.readAllLines(tempDir.resolve("monitor.2.log")));
    assertEquals(List.of(HEADER, row(2)), Files.readAllLines(tempDir.resolve("monitor.3.log")));
    assertEquals(List.of(HEADER, row(3)), Files.readAllLines(tempDir.resolve("monitor.4.log")));
  }

  @Test
  void supportsPathsWithoutExtensions() throws Exception {
    Path output = tempDir.resolve("monitor");
    try (var strategy = new FileMonitorLogWriteStrategy(output, COMMA_SEPARATED)) {
      strategy.write(log(1));
      strategy.rollOut();
      strategy.write(log(2));
    }
    assertEquals(List.of(HEADER, row(2)), Files.readAllLines(tempDir.resolve("monitor.1")));
  }

  @Test
  void closeFlushesAndRejectsFurtherOperations() throws Exception {
    Path output = tempDir.resolve("monitor.log");
    var strategy = new FileMonitorLogWriteStrategy(output, COMMA_SEPARATED);
    strategy.write(log(1));
    strategy.close();
    strategy.close();
    assertEquals(List.of(HEADER, row(1)), Files.readAllLines(output));
    assertThrows(IllegalStateException.class, () -> strategy.write(log(2)));
    assertThrows(IllegalStateException.class, strategy::commit);
    assertThrows(IllegalStateException.class, strategy::rollOut);
  }

  @Test
  void rejectsInvalidConfigurationAndReportsFileErrors() throws Exception {
    Path output = tempDir.resolve("monitor.log");
    assertThrows(IllegalArgumentException.class,
        () -> new FileMonitorLogWriteStrategy(output, Duration.ofSeconds(-1), 0));
    assertThrows(IllegalArgumentException.class,
        () -> new FileMonitorLogWriteStrategy(output, Duration.ZERO, -1));
    assertThrows(IllegalArgumentException.class,
        () -> new FileMonitorLogWriteStrategy(output, Duration.ofSeconds(Long.MAX_VALUE), 0));
    assertThrows(NullPointerException.class, () -> new FileMonitorLogWriteStrategy(null));
    Path parentFile = tempDir.resolve("not-a-directory");
    Files.writeString(parentFile, "occupied");
    try (var strategy = new FileMonitorLogWriteStrategy(parentFile.resolve("monitor.log"))) {
      assertThrows(UncheckedIOException.class, () -> strategy.write(log(1)));
    }
    assertEquals("occupied", Files.readString(parentFile));
  }

  @Test
  void concurrentManualRollOutDoesNotLoseOrDuplicateLogs() throws Exception {
    var executor = Executors.newFixedThreadPool(2);
    CountDownLatch start = new CountDownLatch(1);
    try (var strategy = new FileMonitorLogWriteStrategy(
        tempDir.resolve("monitor.log"), Duration.ZERO, 7, COMMA_SEPARATED)) {
      var writing = executor.submit(() -> {
        start.await();
        for (int i = 0; i < 200; i++) {
          strategy.write(log(i));
        }
        return null;
      });
      var rolling = executor.submit(() -> {
        start.await();
        for (int i = 0; i < 200; i++) {
          strategy.rollOut();
          assertTrue(strategy.commit());
        }
        return null;
      });
      start.countDown();
      writing.get(10, TimeUnit.SECONDS);
      rolling.get(10, TimeUnit.SECONDS);
    } finally {
      executor.shutdownNow();
    }
    List<String> actual = new ArrayList<>();
    try (var files = Files.list(tempDir)) {
      for (Path file : files.toList()) {
        List<String> lines = Files.readAllLines(file);
        assertEquals(HEADER, lines.get(0));
        assertTrue(lines.size() >= 2 && lines.size() <= 8);
        actual.addAll(lines.subList(1, lines.size()));
      }
    }
    List<String> expected = new ArrayList<>();
    for (int i = 0; i < 200; i++) {
      expected.add(row(i));
    }
    assertEquals(expected.size(), actual.size());
    assertEquals(new HashSet<>(expected), new HashSet<>(actual));
  }

  private static MonitorLog log(int id) {
    return new MonitorLog("produce", "message-" + id, "requested", 1000, 2000);
  }

  private static String row(int id) {
    return "produce,message-" + id + ",1000,2000,requested";
  }
}
