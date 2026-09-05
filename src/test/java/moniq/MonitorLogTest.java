package moniq;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import org.junit.jupiter.api.Test;

class MonitorLogTest {

  @Test
  void exposesHeadersAndValuesInExportOrder() {
    MonitorLog log = new MonitorLog("produce", "message-1", "requested", 1_000L, 2_000L);

    assertEquals(
        List.of("RequestType", "Id", "Timestamp", "TimestampNano", "State"),
        log.getHeaders());
    assertEquals(
        List.of("produce", "message-1", "1000", "2000", "requested"),
        log.getValues());
  }
}
