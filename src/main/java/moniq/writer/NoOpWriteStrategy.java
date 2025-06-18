package moniq.writer;

import moniq.MonitorLog;

public class NoOpWriteStrategy implements IMonitorLogWriteStrategy {

    public NoOpWriteStrategy() {
    }

    @Override
    public void write(MonitorLog log) {
    }

    @Override
    public boolean commit() {
        return true;
    }
}
