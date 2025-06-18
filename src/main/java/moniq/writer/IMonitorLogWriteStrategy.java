package moniq.writer;

import moniq.MonitorLog;

public interface IMonitorLogWriteStrategy {
  void write(MonitorLog log);
  boolean commit();
}
