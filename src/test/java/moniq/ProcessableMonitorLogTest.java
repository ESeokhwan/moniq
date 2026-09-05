package moniq;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import org.junit.jupiter.api.Test;

class ProcessableMonitorLogTest {

  @Test
  void transformsTimestampsAndReordersAttributes() {
    MonitorLog plainLog = new MonitorLog("consume", "message-2", "responded", 123L, 456L);
    ProcessableMonitorLog.Wrapper wrapper =
        new ProcessableMonitorLog.Wrapper(
            timestamp -> "millis-" + timestamp,
            timestamp -> "nanos-" + timestamp,
            List.of(
                ProcessableMonitorLog.Attribute.Id,
                ProcessableMonitorLog.Attribute.TimestampNano,
                ProcessableMonitorLog.Attribute.Timestamp));

    ProcessableMonitorLog log = wrapper.wrap(plainLog);

    assertEquals(List.of("Id", "TimestampNano", "Timestamp"), log.getHeaders());
    assertEquals(List.of("message-2", "nanos-456", "millis-123"), log.getValues());
  }
}
