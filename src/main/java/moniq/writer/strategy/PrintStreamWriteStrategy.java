package moniq.writer.strategy;

import moniq.IMonitorLog;

import java.io.PrintStream;

public class PrintStreamWriteStrategy implements IMonitorLogWriteStrategy {

    private final PrintStream outputStream;

    public PrintStreamWriteStrategy(PrintStream outputStream) {
        this.outputStream = outputStream;
    }

    @Override
    public void write(IMonitorLog log) {
        String curLine = String.join(", ", log.getValues());
        outputStream.println(curLine);
    }

    @Override
    public boolean commit() {
        outputStream.flush();
        return true;
    }
}
