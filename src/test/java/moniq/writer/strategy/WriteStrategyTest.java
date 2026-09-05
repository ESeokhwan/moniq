package moniq.writer.strategy;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import moniq.MonitorLog;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class WriteStrategyTest {

  private final MonitorLog log =
      new MonitorLog("produce", "message-1", "requested", 1_000L, 2_000L);

  @Test
  void writesScrapableOutputWithHeader() {
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    ScrapableWriteStrategy strategy = new ScrapableWriteStrategy(output, ",", true);

    strategy.write(log);
    assertTrue(strategy.commit());

    assertEquals(
        "RequestType,Id,Timestamp,TimestampNano,State\n"
            + "produce,message-1,1000,2000,requested\n",
        output.toString(StandardCharsets.UTF_8));
  }

  @Test
  void writesHumanReadableOutput() {
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    ReadFriendlyWriteStrategy strategy = new ReadFriendlyWriteStrategy(output);

    strategy.write(log);
    assertTrue(strategy.commit());

    assertEquals(
        "RequestType: produce, Id: message-1, Timestamp: 1000, "
            + "TimestampNano: 2000, State: requested\n",
        output.toString(StandardCharsets.UTF_8));
  }

  @Test
  void writesCsvOutput(@TempDir Path tempDir) throws Exception {
    Path output = tempDir.resolve("monitor.csv");
    CsvMonitorLogWriteStrategy strategy = new CsvMonitorLogWriteStrategy(output.toString());

    strategy.write(log);
    assertTrue(strategy.commit());

    assertEquals(
        "RequestType,Id,Timestamp,TimestampNano,State\n"
            + "produce,message-1,1000,2000,requested\n",
        Files.readString(output));
  }

  @Test
  void noOpStrategyAcceptsLogs() {
    NoOpWriteStrategy strategy = new NoOpWriteStrategy();

    strategy.write(log);

    assertTrue(strategy.commit());
  }
}
