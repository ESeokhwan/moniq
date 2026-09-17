package moniq.writer.strategy;

import moniq.IMonitorLog;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.UncheckedIOException;

public class CsvMonitorLogWriteStrategy implements IMonitorLogWriteStrategy, AutoCloseable {

  private final String filepath;

  private BufferedWriter writer;
  private boolean closed;

  public CsvMonitorLogWriteStrategy(String filepath) {
    this.filepath = filepath;
  }

  @Override
  public void write(IMonitorLog log) {
    ensureOpen();
    if (writer == null) {
      try {
        this.writer = new BufferedWriter(new FileWriter(filepath));
        String headerLine = String.join(",", log.getHeaders());
        writer.append(headerLine)
              .append("\n");
      } catch (IOException e) {
        throw new UncheckedIOException("Failed to open CSV monitor log " + filepath, e);
      }
    }
    
    try {
      String curLine = String.join(",", log.getValues());
      writer.append(curLine)
            .append("\n");
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to write CSV monitor log " + filepath, e);
    }
  }

  @Override
  public boolean commit() {
    ensureOpen();
    if (writer == null) {
      return true;
    }
    try {
      this.writer.flush();
    } catch (IOException e) {
      return false;
    }
    return true;
  }

  /** Flushes and closes the current CSV file. Repeated calls are harmless. */
  @Override
  public void close() {
    if (closed) {
      return;
    }
    try {
      if (writer != null) {
        writer.close();
      }
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to close CSV monitor log " + filepath, e);
    } finally {
      closed = true;
    }
  }

  private void ensureOpen() {
    if (closed) {
      throw new IllegalStateException("CsvMonitorLogWriteStrategy is closed");
    }
  }
  
}
