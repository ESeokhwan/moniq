package moniq.writer.strategy;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import moniq.IMonitorLog;
import org.junit.jupiter.api.Test;

class CompositeMonitorLogWriteStrategyTest {

  private static final IMonitorLog LOG = new StaticLog();

  @Test
  void writesEveryStrategyInConstructionOrder() {
    List<String> events = new ArrayList<>();
    RecordingStrategy first = new RecordingStrategy("first", events, true);
    RecordingStrategy second = new RecordingStrategy("second", events, true);
    CompositeMonitorLogWriteStrategy strategy =
        new CompositeMonitorLogWriteStrategy(first, second);

    strategy.write(LOG);

    assertTrue(strategy.commit());
    assertEquals(
        List.of("first.write", "second.write", "first.commit", "second.commit"), events);
  }

  @Test
  void stopsWritingAfterAChildFailsAndIncludesItsIdentity() {
    List<String> events = new ArrayList<>();
    RecordingStrategy first = new RecordingStrategy("first", events, true);
    RuntimeException cause = new IllegalStateException("destination unavailable");
    FailingWriteStrategy failing = new FailingWriteStrategy(cause);
    RecordingStrategy afterFailure = new RecordingStrategy("after", events, true);
    CompositeMonitorLogWriteStrategy strategy =
        new CompositeMonitorLogWriteStrategy(first, failing, afterFailure);

    CompositeMonitorLogWriteStrategy.WriteException error =
        assertThrows(CompositeMonitorLogWriteStrategy.WriteException.class, () -> strategy.write(LOG));

    assertEquals(1, error.strategyIndex());
    assertSame(failing, error.strategy());
    assertSame(cause, error.getCause());
    assertEquals(List.of("first.write"), events);
  }

  @Test
  void commitsEveryChildWhenAnEarlierCommitReturnsFalse() {
    List<String> events = new ArrayList<>();
    CompositeMonitorLogWriteStrategy strategy =
        new CompositeMonitorLogWriteStrategy(
            new RecordingStrategy("first", events, false),
            new RecordingStrategy("second", events, true),
            new RecordingStrategy("third", events, false));

    assertFalse(strategy.commit());

    assertEquals(List.of("first.commit", "second.commit", "third.commit"), events);
  }

  @Test
  void commitsRemainingChildrenBeforeReportingACommitFailure() {
    List<String> events = new ArrayList<>();
    RuntimeException cause = new IllegalStateException("first commit failed");
    FailingCommitStrategy failing = new FailingCommitStrategy(events, cause);
    RecordingStrategy afterFailure = new RecordingStrategy("after", events, true);
    CompositeMonitorLogWriteStrategy strategy =
        new CompositeMonitorLogWriteStrategy(failing, afterFailure);

    CompositeMonitorLogWriteStrategy.CommitException error =
        assertThrows(CompositeMonitorLogWriteStrategy.CommitException.class, strategy::commit);

    assertEquals(0, error.strategyIndex());
    assertSame(failing, error.strategy());
    assertSame(cause, error.getCause());
    assertEquals(List.of("failing.commit", "after.commit"), events);
  }

  @Test
  void closesCloseableChildrenInReverseOrderAndRejectsFurtherUse() {
    List<String> events = new ArrayList<>();
    CloseableRecordingStrategy first = new CloseableRecordingStrategy("first", events);
    CloseableRecordingStrategy second = new CloseableRecordingStrategy("second", events);
    CompositeMonitorLogWriteStrategy strategy =
        new CompositeMonitorLogWriteStrategy(first, second);

    strategy.close();
    strategy.close();

    assertEquals(List.of("second.close", "first.close"), events);
    assertThrows(IllegalStateException.class, () -> strategy.write(LOG));
    assertThrows(IllegalStateException.class, strategy::commit);
  }

  @Test
  void rejectsMissingChildren() {
    assertThrows(IllegalArgumentException.class, () -> new CompositeMonitorLogWriteStrategy());
    assertThrows(
        NullPointerException.class,
        () -> new CompositeMonitorLogWriteStrategy(new IMonitorLogWriteStrategy[] {null}));
  }

  private static class RecordingStrategy implements IMonitorLogWriteStrategy {
    protected final String name;
    protected final List<String> events;
    private final boolean commitResult;

    private RecordingStrategy(String name, List<String> events, boolean commitResult) {
      this.name = name;
      this.events = events;
      this.commitResult = commitResult;
    }

    @Override
    public void write(IMonitorLog log) {
      events.add(name + ".write");
    }

    @Override
    public boolean commit() {
      events.add(name + ".commit");
      return commitResult;
    }
  }

  private static final class FailingWriteStrategy implements IMonitorLogWriteStrategy {
    private final RuntimeException failure;

    private FailingWriteStrategy(RuntimeException failure) {
      this.failure = failure;
    }

    @Override
    public void write(IMonitorLog log) {
      throw failure;
    }

    @Override
    public boolean commit() {
      return true;
    }
  }

  private static final class FailingCommitStrategy implements IMonitorLogWriteStrategy {
    private final List<String> events;
    private final RuntimeException failure;

    private FailingCommitStrategy(List<String> events, RuntimeException failure) {
      this.events = events;
      this.failure = failure;
    }

    @Override
    public void write(IMonitorLog log) {
    }

    @Override
    public boolean commit() {
      events.add("failing.commit");
      throw failure;
    }
  }

  private static final class CloseableRecordingStrategy extends RecordingStrategy
      implements AutoCloseable {

    private CloseableRecordingStrategy(String name, List<String> events) {
      super(name, events, true);
    }

    @Override
    public void close() {
      events.add(name + ".close");
    }
  }

  private static final class StaticLog implements IMonitorLog {
    @Override
    public void preprocess() {
    }

    @Override
    public List<String> getHeaders() {
      return List.of("Type");
    }

    @Override
    public List<String> getValues() {
      return List.of("value");
    }
  }
}
