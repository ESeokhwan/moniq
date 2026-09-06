package moniq.writer;

import moniq.IMonitorLog;
import moniq.exception.MoniqRuntimeException;

@FunctionalInterface
public interface MonitorLogErrorHandler {

  void handle(IMonitorLog log, Throwable error);

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
