package moniq;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class MonitorQueue {

  private final Queue<IMonitorLog> queue;
  
  private final AtomicInteger size = new AtomicInteger(0);

  public MonitorQueue() {
    this.queue = new ConcurrentLinkedQueue<IMonitorLog>();
  }

  public boolean enqueue(IMonitorLog log) {
    boolean res = queue.add(log);
    size.incrementAndGet();
    return res;
  }

  public IMonitorLog dequeue() {
    IMonitorLog res = queue.poll();
    if (res != null) {
      size.decrementAndGet();
    }
    return res;
  }

  public int size() {
    return size.get();
  }

  public boolean isEmpty() {
    return queue.isEmpty();
  }
}
