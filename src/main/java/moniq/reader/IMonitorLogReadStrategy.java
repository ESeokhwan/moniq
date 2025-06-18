package moniq.reader;

import moniq.MonitorLog;

import java.util.List;

public interface IMonitorLogReadStrategy {
  void read(List<MonitorLog> tar);
}
