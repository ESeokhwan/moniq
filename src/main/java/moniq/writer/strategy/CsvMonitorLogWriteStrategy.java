package moniq.writer.strategy;

import moniq.IMonitorLog;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public class CsvMonitorLogWriteStrategy implements IMonitorLogWriteStrategy {

  private final String filepath;

  private BufferedWriter writer;

  public CsvMonitorLogWriteStrategy(String filepath) {
    this.filepath = filepath;
  }

  @Override
  public void write(IMonitorLog log) {
    if (writer == null) {
      try {
        this.writer = new BufferedWriter(new FileWriter(filepath));
        String headerLine = String.join(",", log.getHeaders());
        writer.append(headerLine)
              .append("\n");
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    
    try {
      String curLine = String.join(",", log.getValues());
      writer.append(curLine)
            .append("\n");
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public boolean commit() {
    try {
      this.writer.flush();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return true;
  }
  
}
