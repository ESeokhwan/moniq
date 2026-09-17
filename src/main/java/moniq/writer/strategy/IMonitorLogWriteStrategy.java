package moniq.writer.strategy;

import moniq.IMonitorLog;

public interface IMonitorLogWriteStrategy {

  /**
   * Writes one preprocessed log.
   *
   * <p>An implementation may have written part of its output before throwing. Callers must still
   * invoke {@link #commit()} for that batch so successfully written output can be flushed.
   */
  void write(IMonitorLog log);

  /**
   * Makes output from the current batch visible.
   *
   * <p>This method must be safe when no log has been written. Return {@code false} when the commit
   * could not complete; throw only for an unrecoverable failure.
   */
  boolean commit();
}
