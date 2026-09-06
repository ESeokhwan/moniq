package moniq;

/** A monitor log that measures elapsed epoch-millisecond time for a message. */
public interface ILatencyMonitorLog extends IMonitorLog {

  /** Returns the extracted message identifier. */
  String getContent();

  /** Returns the request creation time in epoch milliseconds. */
  long getRequestedAt();

  /** Returns the response observation time in epoch milliseconds. */
  long getRespondedAt();

  /** Returns {@code respondedAt - requestedAt} in milliseconds. */
  long getLatency();
}
