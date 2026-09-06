package moniq.writer;

import moniq.IMonitorLog;
import moniq.exception.MoniqRuntimeException;

/** Handles preprocessing or write failures for an individual monitor log. */
@FunctionalInterface
public interface MonitorLogErrorHandler {

  /** Handles a failed log; returning skips that log, while throwing stops the writer. */
  void handle(IMonitorLog log, Throwable error);

  /** Returns the default handler, which propagates the original failure. */
  static MonitorLogErrorHandler rethrowing() {
    return (log, error) -> {
      if (error instanceof RuntimeException runtimeException) {
        throw runtimeException;
      }
      if (error instanceof Error fatalError) {
        throw fatalError;
      }
      throw new MoniqRuntimeException("Failed to process a monitor log.", error);
    };
  }
}
