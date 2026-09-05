package moniq;

import java.util.List;

public interface IMonitorLog {
    default void preprocess() {
    }

    List<String> getHeaders();
    List<String> getValues();
}
