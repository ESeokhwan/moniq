package moniq.writer.strategy;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;
import moniq.IMonitorLog;

/**
 * Writes UTF-8 log files, rolling out on elapsed time, log count, or {@link #rollOut()}.
 *
 * <p>Automatic limits are checked before each write. A new file is created only when a log is
 * available, so idle periods and repeated manual requests do not create empty files. Comma-separated
 * files include a header; the header does not count toward the log limit. Use logs with the same
 * column layout in one strategy when using comma-separated output.
 *
 * <p>For {@code monitor.log}, files are named {@code monitor.log}, {@code monitor.1.log}, and so on.
 * Existing names are skipped atomically, including when another strategy uses the same base path.
 * All lifecycle operations are synchronized so a user-input thread can request a roll-out while
 * {@code MonitorLogWriter} writes. The caller must close this strategy after the writer has stopped.
 */
public final class FileMonitorLogWriteStrategy implements IMonitorLogWriteStrategy, AutoCloseable {

  public enum Format {
    /** One log per line, with labeled values: {@code RequestType: produce, Id: message-1, ...}. */
    READ_FRIENDLY,
    /** A header followed by comma-separated records, with quoting for CSV conversion. */
    COMMA_SEPARATED
  }

  private final Path filePath;
  private final Format format;
  private final long rollOutIntervalNanos;
  private final long maxLogsPerFile;
  private final LongSupplier nanoTime;

  private BufferedWriter writer;
  private long nextFileIndex;
  private long fileOpenedAtNanos;
  private long logsInFile;
  private boolean closed;

  /** Creates a human-readable strategy with manual roll-out only. */
  public FileMonitorLogWriteStrategy(Path filePath) {
    this(filePath, Format.READ_FRIENDLY);
  }

  /** Creates a strategy with the selected format and manual roll-out only. */
  public FileMonitorLogWriteStrategy(Path filePath, Format format) {
    this(filePath, Duration.ZERO, 0, format);
  }

  /** Creates a human-readable strategy that rolls out when either enabled limit is reached. */
  public FileMonitorLogWriteStrategy(
      Path filePath, Duration rollOutInterval, long maxLogsPerFile) {
    this(filePath, rollOutInterval, maxLogsPerFile, Format.READ_FRIENDLY);
  }

  /**
   * Creates a strategy that rolls out when either enabled limit is reached.
   *
   * @param filePath base output path; missing parent directories are created on the first write
   * @param rollOutInterval maximum file age measured with a monotonic clock; zero disables it
   * @param maxLogsPerFile maximum number of logs per file; zero disables it
   * @param format human-readable labeled records or comma-separated records
   * @throws IllegalArgumentException if a limit is negative or the interval cannot fit in nanoseconds
   */
  public FileMonitorLogWriteStrategy(
      Path filePath, Duration rollOutInterval, long maxLogsPerFile, Format format) {
    this(filePath, rollOutInterval, maxLogsPerFile, format, System::nanoTime);
  }

  FileMonitorLogWriteStrategy(
      Path filePath, Duration rollOutInterval, long maxLogsPerFile,
      Format format, LongSupplier nanoTime) {
    this.filePath = Objects.requireNonNull(filePath, "filePath must not be null")
        .toAbsolutePath().normalize();
    if (this.filePath.getFileName() == null) {
      throw new IllegalArgumentException("filePath must name a file");
    }
    Objects.requireNonNull(rollOutInterval, "rollOutInterval must not be null");
    if (rollOutInterval.isNegative() || maxLogsPerFile < 0) {
      throw new IllegalArgumentException("Roll-out limits must not be negative");
    }
    try {
      this.rollOutIntervalNanos = rollOutInterval.toNanos();
    } catch (ArithmeticException e) {
      throw new IllegalArgumentException("rollOutInterval is too large", e);
    }
    this.maxLogsPerFile = maxLogsPerFile;
    this.format = Objects.requireNonNull(format, "format must not be null");
    this.nanoTime = Objects.requireNonNull(nanoTime, "nanoTime must not be null");
  }

  @Override
  public synchronized void write(IMonitorLog log) {
    ensureOpen();
    Objects.requireNonNull(log, "log must not be null");
    // Resolve the record before changing files, in case deferred log access fails.
    List<String> headers = log.getHeaders();
    List<String> values = log.getValues();
    String header = format == Format.COMMA_SEPARATED ? commaSeparatedLine(headers) : null;
    String record = format == Format.COMMA_SEPARATED
        ? commaSeparatedLine(values) : readFriendlyLine(headers, values);
    try {
      if (writer != null && shouldRollOut()) {
        closeCurrentFile();
      }
      if (writer == null) {
        openNextFile();
      }
      if (logsInFile == 0 && header != null) {
        writer.write(header);
        writer.write('\n');
      }
      writer.write(record);
      writer.write('\n');
      logsInFile++;
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to write monitor log to " + filePath, e);
    }
  }

  /** Flushes the current file; returns false if flushing fails. No file is created by a commit. */
  @Override
  public synchronized boolean commit() {
    ensureOpen();
    try {
      if (writer != null) {
        writer.flush();
      }
      return true;
    } catch (IOException e) {
      return false;
    }
  }

  /**
   * Flushes and closes the current file. The next write opens a new file and resets both limits.
   * Can be called directly from a console command, UI event, or other user-input handler.
   */
  public synchronized void rollOut() {
    ensureOpen();
    try {
      closeCurrentFile();
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to roll out monitor log file " + filePath, e);
    }
  }

  /** Flushes and releases the file. Repeated close calls are harmless. */
  @Override
  public synchronized void close() {
    if (closed) {
      return;
    }
    try {
      closeCurrentFile();
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to close monitor log file " + filePath, e);
    } finally {
      closed = true;
    }
  }

  private boolean shouldRollOut() {
    return (maxLogsPerFile > 0 && logsInFile >= maxLogsPerFile)
        || (rollOutIntervalNanos > 0
            && nanoTime.getAsLong() - fileOpenedAtNanos >= rollOutIntervalNanos);
  }

  private void openNextFile() throws IOException {
    Files.createDirectories(filePath.getParent());
    while (writer == null) {
      Path candidate = indexedPath(nextFileIndex);
      try {
        writer = Files.newBufferedWriter(
            candidate, StandardCharsets.UTF_8, StandardOpenOption.CREATE_NEW,
            StandardOpenOption.WRITE);
      } catch (FileAlreadyExistsException e) {
        nextFileIndex++;
        continue;
      }
      nextFileIndex++;
    }
    logsInFile = 0;
    fileOpenedAtNanos = nanoTime.getAsLong();
  }

  private Path indexedPath(long index) {
    if (index == 0) {
      return filePath;
    }
    String filename = filePath.getFileName().toString();
    int dot = filename.lastIndexOf('.');
    if (dot <= 0) {
      return filePath.resolveSibling(filename + "." + index);
    }
    return filePath.resolveSibling(
        filename.substring(0, dot) + "." + index + filename.substring(dot));
  }

  private void closeCurrentFile() throws IOException {
    if (writer != null) {
      BufferedWriter current = writer;
      writer = null;
      current.close();
    }
  }

  private void ensureOpen() {
    if (closed) {
      throw new IllegalStateException("FileMonitorLogWriteStrategy is closed");
    }
  }

  private static String readFriendlyLine(List<String> headers, List<String> values) {
    StringBuilder line = new StringBuilder();
    for (int i = 0; i < values.size(); i++) {
      if (i > 0) {
        line.append(", ");
      }
      String label = i < headers.size() ? headers.get(i) : "Column" + (i + 1);
      line.append(singleLine(label)).append(": ").append(singleLine(values.get(i)));
    }
    return line.toString();
  }

  private static String singleLine(String value) {
    return Objects.requireNonNull(value, "Log column must not be null")
        .replace("\\", "\\\\").replace("\r", "\\r").replace("\n", "\\n");
  }

  private static String commaSeparatedLine(List<String> columns) {
    return columns.stream().map(FileMonitorLogWriteStrategy::escapeCsv)
        .collect(Collectors.joining(","));
  }

  private static String escapeCsv(String value) {
    Objects.requireNonNull(value, "CSV column must not be null");
    if (value.indexOf(',') >= 0 || value.indexOf('"') >= 0
        || value.indexOf('\n') >= 0 || value.indexOf('\r') >= 0) {
      return "\"" + value.replace("\"", "\"\"") + "\"";
    }
    return value;
  }
}
