package moniq;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class ProcessableMonitorLog implements IMonitorLog {

  public enum Attribute {
    RequestType, Id, Timestamp, TimestampNano, State
  }

  private final MonitorLog plainLog;

  private final ITimestampProcessor timestampProcessor;

  private final ITimestampProcessor timestampNanoProcessor;

  private final List<Attribute> attributeOrders;

  public ProcessableMonitorLog(MonitorLog plainLog, ITimestampProcessor timestampProcessor, ITimestampProcessor timestampNanoProcessor, List<Attribute> attributeOrders) {
    this.plainLog = plainLog;
    this.timestampProcessor = timestampProcessor;
    this.timestampNanoProcessor = timestampNanoProcessor;
    this.attributeOrders = attributeOrders;
  }

  public String getType() {
    return plainLog.getType();
  }

  public String getId() {
    return plainLog.getId();
  }

  public String getState() {
    return plainLog.getState();
  }

  public String getTimestamp() {
    return timestampProcessor.processTimestamp(plainLog.getTimestamp());
  }

  public String getTimestampNano() {
      return timestampNanoProcessor.processTimestamp(plainLog.getTimestampNano());
  }

  @Override
  public int hashCode() {
    return plainLog.hashCode();
  }

  @Override
  public boolean equals(Object oth) {
    if (this == oth) return true;
    if (oth == null || getClass() != oth.getClass()) return false;

    ProcessableMonitorLog converted = (ProcessableMonitorLog) oth;
    return Objects.equals(plainLog, converted.plainLog);
  }

  public String toString() {
    return "MonitorLog{" +
            "type='" + getType() + '\'' +
            ", id='" + getId() + '\'' +
            ", state='" + getState() + '\'' +
            ", timestamp=" + getTimestamp() +
            ", timestampNano=" + getTimestampNano() +
            ", attributeOrders=" + attributeOrders +
            '}';
  }

  @Override
  public List<String> getHeaders() {
    List<String> headers = new ArrayList<>();
    for (Attribute order : attributeOrders) {
      switch (order) {
        case RequestType:
          headers.add("RequestType");
          break;
        case Id:
          headers.add("Id");
          break;
        case Timestamp:
          headers.add("Timestamp");
          break;
        case TimestampNano:
          headers.add("TimestampNano");
          break;
        case State:
          headers.add("State");
          break;
      }
    }
    return headers;
  }

  @Override
  public List<String> getValues() {
    List<String> values = new ArrayList<>();
    for (Attribute order : attributeOrders) {
      switch (order) {
        case RequestType:
          values.add(getType());
          break;
        case Id:
          values.add(getId());
          break;
        case Timestamp:
          values.add(getTimestamp());
          break;
        case TimestampNano:
          values.add(getTimestampNano());
          break;
        case State:
          values.add(getState());
          break;
      }
    }
    return values;
  }

  public interface ITimestampProcessor {
    String processTimestamp(long timestamp);
  }

  public static class Wrapper {
    private final ITimestampProcessor timestampProcessor;
    private final ITimestampProcessor timestampNanoProcessor;
    private final List<Attribute> attributeOrders;

    public Wrapper(ITimestampProcessor timestampProcessor, ITimestampProcessor timestampNanoProcessor, List<Attribute> attributeOrders) {
      this.timestampProcessor = timestampProcessor;
      this.timestampNanoProcessor = timestampNanoProcessor;
      this.attributeOrders = attributeOrders;
    }

    public Wrapper withTimestampProcessor(ITimestampProcessor timestampProcessor) {
      return new Wrapper(timestampProcessor, this.timestampNanoProcessor, this.attributeOrders);
    }

    public Wrapper withTimestampNanoProcessor(ITimestampProcessor timestampNanoProcessor) {
      return new Wrapper(this.timestampProcessor, timestampNanoProcessor, this.attributeOrders);
    }

    public Wrapper withAttributeOrders(List<Attribute> attributeOrders) {
      return new Wrapper(this.timestampProcessor, this.timestampNanoProcessor, attributeOrders);
    }

    public Wrapper toPrettierTimestampWrapper() {
      return this
              .withTimestampProcessor(timestamp -> String.format("%1$tY-%1$tm-%1$tdT%1$tH:%1$tM:%1$tS.%1$tL", timestamp))
              .withTimestampNanoProcessor(timestampNano -> String.valueOf(timestampNano % 1_000_000));
    }

    public ProcessableMonitorLog wrap(MonitorLog plainLog) {
      return new ProcessableMonitorLog(plainLog, timestampProcessor, timestampNanoProcessor, attributeOrders);
    }
  }
}
