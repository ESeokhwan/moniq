package moniq;

public interface ILatencyMonitorLog extends IMonitorLog {

  String getContent();

  long getRequestedAt();

  long getRespondedAt();

  long getLatency();
}
