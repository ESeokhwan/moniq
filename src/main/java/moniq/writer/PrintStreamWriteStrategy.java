package moniq.writer;

import moniq.MonitorLog;

import java.io.PrintStream;

public class PrintStreamWriteStrategy implements IMonitorLogWriteStrategy {

    private final PrintStream outputStream;

    public PrintStreamWriteStrategy(PrintStream outputStream) {
        this.outputStream = outputStream;
    }

    @Override
    public void write(MonitorLog log) {
        outputStream.println(log.getType() + ", " + log.getId() + ", " + log.getTimestamp() + ", " + log.getState());
    }

    @Override
    public boolean commit() {
        outputStream.flush();
        return true;
    }
}
