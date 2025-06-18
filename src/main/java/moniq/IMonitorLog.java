package moniq;

import java.util.List;

public interface IMonitorLog {
    List<String> getHeaders();
    List<String> getValues();
}
