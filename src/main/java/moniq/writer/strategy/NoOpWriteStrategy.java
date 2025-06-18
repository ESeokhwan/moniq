package moniq.writer.strategy;

import moniq.IMonitorLog;

public class NoOpWriteStrategy implements IMonitorLogWriteStrategy {

    public NoOpWriteStrategy() {
    }

    @Override
    public void write(IMonitorLog log) {
    }

    @Override
    public boolean commit() {
        return true;
    }
}
