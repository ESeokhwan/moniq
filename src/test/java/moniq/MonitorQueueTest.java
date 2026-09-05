package moniq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class MonitorQueueTest {

  @Test
  void dequeuesLogsInFifoOrder() {
    MonitorQueue queue = new MonitorQueue();
    MonitorLog first = new MonitorLog("type", "first", "state", 1L, 10L);
    MonitorLog second = new MonitorLog("type", "second", "state", 2L, 20L);

    queue.enqueue(first);
    queue.enqueue(second);

    assertEquals(2, queue.size());
    assertEquals(first, queue.dequeue());
    assertEquals(second, queue.dequeue());
    assertNull(queue.dequeue());
    assertEquals(0, queue.size());
    assertTrue(queue.isEmpty());
  }
}
