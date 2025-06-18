package moniq.writer.strategy;

import moniq.IMonitorLog;

public interface IMonitorLogWriteStrategy {
  void write(IMonitorLog log);
  boolean commit();
}
