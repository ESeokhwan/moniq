package moniq;

import java.util.List;

/** A record that can be preprocessed and exported as ordered string columns. */
public interface IMonitorLog {

  /** Performs deferred work before the log is handed to a write strategy. */
  default void preprocess() {
  }

  /** Returns column names in the same order as {@link #getValues()}. */
  List<String> getHeaders();

  /** Returns exportable column values after preprocessing. */
  List<String> getValues();
}
