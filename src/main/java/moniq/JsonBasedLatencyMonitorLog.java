package moniq;

import java.util.List;
import java.util.Objects;
import moniq.exception.NotProcessedException;
import moniq.util.ILatencyMonitoringMessageAdaptor;

public class JsonBasedLatencyMonitorLog implements ILatencyMonitorLog {

  private static final List<String> HEADERS =
      List.of("Content", "Status", "RequestedAt", "RespondedAt", "Latency");

  private final ILatencyMonitoringMessageAdaptor messageAdaptor;
  private final String rawData;
  private final String status;
  private final long respondedAt;

  private volatile ExtractedValues extractedValues;

  public JsonBasedLatencyMonitorLog(
      ILatencyMonitoringMessageAdaptor messageAdaptor,
      String rawData,
      String status,
      long respondedAt) {
    this.messageAdaptor = Objects.requireNonNull(messageAdaptor, "messageAdaptor must not be null");
    this.rawData = Objects.requireNonNull(rawData, "rawData must not be null");
    this.status = Objects.requireNonNull(status, "status must not be null");
    this.respondedAt = respondedAt;
  }

  @Override
  public void preprocess() {
    String content = messageAdaptor.extractMessageId(rawData);
    long requestedAt = messageAdaptor.extractRequestedAt(rawData);
    extractedValues = new ExtractedValues(content, requestedAt);
  }

  @Override
  public List<String> getHeaders() {
    return HEADERS;
  }

  @Override
  public List<String> getValues() {
    ExtractedValues extracted = requireExtractedValues();
    return List.of(
        extracted.content(),
        status,
        Long.toString(extracted.requestedAt()),
        Long.toString(respondedAt),
        Long.toString(respondedAt - extracted.requestedAt()));
  }

  public String getRawData() {
    return rawData;
  }

  public String getStatus() {
    return status;
  }

  @Override
  public String getContent() {
    return requireExtractedValues().content();
  }

  @Override
  public long getRequestedAt() {
    return requireExtractedValues().requestedAt();
  }

  @Override
  public long getRespondedAt() {
    return respondedAt;
  }

  @Override
  public long getLatency() {
    return respondedAt - requireExtractedValues().requestedAt();
  }

  private ExtractedValues requireExtractedValues() {
    ExtractedValues extracted = extractedValues;
    if (extracted == null) {
      throw new NotProcessedException();
    }
    return extracted;
  }

  private record ExtractedValues(String content, long requestedAt) {
  }
}
