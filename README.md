# moniq

`moniq` is a Java library for collecting performance-monitoring records in memory and exporting
them in batches. Producers submit lightweight `IMonitorLog` objects to a `MonitorLogWriter`; the
writer preprocesses them and sends them to a configurable write strategy.

The project requires Java 17. Run the test suite with:

```shell
./gradlew clean test
```

## Basic usage

```java
MonitorQueue queue = new MonitorQueue();
IMonitorLogWriteStrategy strategy = new ScrapableWriteStrategy(System.out);
MonitorLogWriter writer = new MonitorLogWriter(
    queue,
    strategy,
    BatchPolicy.fixedSize(1_000),
    FlushPolicy.after(Duration.ofSeconds(1)),
    4);

Thread writerThread = new Thread(writer, "moniq-writer");
writerThread.start();

writer.submit(new MonitorLog(
    "produce",
    "message-1",
    "requested",
    System.currentTimeMillis(),
    System.nanoTime()));

// Stop accepting submissions, drain queued logs, flush, and wait for completion.
writer.gracefulShutdown();
writerThread.join();
```

`submit()` is the preferred producer API because enqueueing and wake-up are coordinated. Existing
code can continue to manage the queue directly:

```java
queue.enqueue(log);
writer.notifyIfNeeded();
```

Call `notifyIfNeeded()` after every direct enqueue. This is also required for an incomplete batch to
start its timeout.

`submit()` does not acquire a shared exclusive lock. It registers the call as in flight, checks that
the writer is accepting data, performs the concurrent-queue enqueue, and wakes the writer with
`LockSupport.unpark()`. Concurrent producer signals are coalesced into a single pending wake-up.

## Writer batching and lifecycle

`MonitorLogWriter` is a one-shot `Runnable`: create one writer for one writer thread and do not call
`run()` again after it exits. Calling `submit()` after `gracefulShutdown()` throws an
`IllegalStateException`.

Batch behavior is controlled by explicit `BatchPolicy` and `FlushPolicy` values:

| Configuration | Behavior |
| --- | --- |
| `fixedSize(n)`, `disabled()` | Flush when `n` logs accumulate |
| `fixedSize(n)`, `after(duration)` | Flush at `n` logs or when the pending batch times out |
| `unbounded()`, `after(duration)` | Flush all currently pending logs on timeout |
| `unbounded()`, `disabled()` | Hold all logs until shutdown |

`fixedSize()` and `after()` accept only positive values. Disabling a timeout or removing the size
boundary is represented by a named policy rather than a numeric sentinel:

```java
BatchPolicy fixed = BatchPolicy.fixedSize(1_000);
BatchPolicy unbounded = BatchPolicy.unbounded();
FlushPolicy timed = FlushPolicy.after(Duration.ofSeconds(1));
FlushPolicy noTimeout = FlushPolicy.disabled();
```

The older numeric constructors remain available for source compatibility but are deprecated. They
translate a negative batch size to `unbounded()` and `Duration.ZERO` to `disabled()`.

Call `flush()` to bypass both policies and synchronously process all currently queued logs:

```java
writer.submit(log);
boolean committed = writer.flush();
```

`flush()` inserts a FIFO boundary into the writer queue and returns only after every log before that
boundary finishes preprocessing, ordered writes, and the strategy's `commit()`. Its return value is
`false` if any commit from the preceding boundary to this one fails; a boundary with no preceding
writes returns `true` without committing.
Logs submitted after the boundary remain for the next batch. Concurrent `submit()` and `flush()`
calls are ordered by their lock-free queue insertion, so calls that overlap may fall on either side
of the boundary. Afterward, the next queued log begins a new batch and a new timeout period. A file
strategy's roll-out age and record count are independent and remain unchanged. To create an explicit
file boundary, use `flushAndRun()` so the roll-out runs on the writer thread immediately after its
commit:

```java
writer.flushAndRun(fileStrategy.rollOutAction());
```

Each flush keeps its own FIFO boundary. Calling `flush()` after shutdown starts throws
`IllegalStateException`; interruption while waiting throws `InterruptedException`, but the accepted
flush request still runs.

On shutdown, the writer wakes automatically, stops accepting `submit()` calls, drains queued logs,
flushes the strategy, and terminates. The caller still owns the writer thread and must `join()` it.
Submissions that had already passed the acceptance check are counted as in flight, so shutdown does
not terminate the writer until those submissions finish and their logs have been drained. Direct
calls to `MonitorQueue.enqueue()` bypass this lifecycle guarantee; prefer `submit()` when producers
can race with shutdown.

## Preprocessing workers and errors

The optional `workerCount` controls a reusable preprocessing thread pool. `preprocess()` calls may
run concurrently, but `write()` calls remain serialized in queue order and `commit()` runs on the
writer thread. Write strategies therefore do not need to support concurrent calls from a single
writer.

The default error handler rethrows preprocessing and write failures, so the writer never silently
drops a failed log. Supply a handler to report an error and continue with the remaining records:

```java
MonitorLogWriter writer = new MonitorLogWriter(
    queue,
    strategy,
    BatchPolicy.fixedSize(1_000),
    FlushPolicy.after(Duration.ofSeconds(1)),
    4,
    (log, error) -> error.printStackTrace());
```

If a custom handler returns normally, the failed log is skipped and processing continues. If it
throws, the writer exits after its final flush.

## Latency monitoring messages

Latency messages contain an ID and the epoch-millisecond time at which a request was created. The
receiver records its response time in the same clock domain, and `JsonBasedLatencyMonitorLog`
calculates:

```text
latency = respondedAt - requestedAt
```

### JSON format

`JsonBasedLatencyMonitoringMessageGenerator` stores metadata and random payload in one JSON object:

```json
{"id":"message-1","requested_at":"1720000000000","payload":"...","source":"gateway-a"}
```

```java
JsonBasedLatencyMonitoringMessageGenerator adaptor =
    new JsonBasedLatencyMonitoringMessageGenerator(1_024, 8_192);

String message = adaptor.generate(
    "message-1",
    System.currentTimeMillis(),
    Map.of("source", "gateway-a"));
```

Use this format when the whole message should be valid JSON.

### Fast JSON format

`FastJsonBasedLatencyMonitoringMessageGenerator` keeps the random payload outside the parsed JSON:

```text
{"id":"message-1","requested_at":"1720000000000","source":"gateway-a"}!...
```

Only the metadata before the final `!` is parsed at the receiver. Generated payloads use
alphanumeric characters and never contain the delimiter. Use this format when payloads are large
and only their metadata is needed for monitoring.

Both formats:

- escape JSON values through Jackson;
- generate `requested_at` as a string and also accept an integer when extracting C++ messages;
- reject `id`, `requested_at`, and `payload` as additional metadata keys;
- throw `InvalidMessageException` for malformed or incomplete input;
- provide extract-only adaptors when the receiving side does not generate messages.

Payload sizes count Java characters, not encoded UTF-8 bytes.

## Creating latency logs

Use the matching adaptor to create a log from the received raw message:

```java
ILatencyMonitoringMessageAdaptor adaptor =
    new FastExtractOnlyJsonBasedLatencyMonitoringMessageAdaptor();

JsonBasedLatencyMonitorLog log = new JsonBasedLatencyMonitorLog(
    adaptor,
    receivedMessage,
    "success",
    System.currentTimeMillis());

writer.submit(log);
```

The writer invokes `preprocess()` before writing. A latency log exposes these columns:

```text
Content, Status, RequestedAt, RespondedAt, Latency
```

Calling `getContent()`, `getRequestedAt()`, `getLatency()`, or `getValues()` before preprocessing
throws `NotProcessedException`.

## Write strategies

The included strategies are:

- `CsvMonitorLogWriteStrategy` for CSV files;
- `FileMonitorLogWriteStrategy` for readable or comma-separated UTF-8 log files with roll-out;
- `CompositeMonitorLogWriteStrategy` for ordered fan-out to multiple destinations;
- `ScrapableWriteStrategy` for delimiter-separated output with an optional header;
- `ReadFriendlyWriteStrategy` for labeled human-readable output;
- `NoOpWriteStrategy` for measuring monitoring overhead without output.

Custom exporters implement `IMonitorLogWriteStrategy`. `write()` receives preprocessed logs and
`commit()` is called at flush boundaries. Implementations must allow an empty `commit()`; it must
return `false` when flushing fails.

### Multiple destinations

Use `CompositeMonitorLogWriteStrategy` to send each log to independent destinations in construction
order. It is fan-out, not a transaction: if a later destination fails during `write()`, earlier
destinations may already contain that log. Its `commit()` still commits every destination and returns
`false` when any destination returns `false`.

```java
FileMonitorLogWriteStrategy file = new FileMonitorLogWriteStrategy(Path.of("logs", "monitor.log"));
try (CompositeMonitorLogWriteStrategy strategy = new CompositeMonitorLogWriteStrategy(
    new ReadFriendlyWriteStrategy(System.out), file)) {
  MonitorLogWriter writer = new MonitorLogWriter(
      new MonitorQueue(), strategy, BatchPolicy.fixedSize(1_000));
  // Start, submit to, gracefully shut down, and join writer before closing strategy.
  // For a manual file boundary: writer.flushAndRun(file.rollOutAction());
}
```

`close()` closes `AutoCloseable` child strategies in reverse order. Keep a reference to a file
strategy when only that file needs roll-out; roll-out is intentionally not part of the common
write-strategy interface.

### Rolling log files

```java
try (FileMonitorLogWriteStrategy strategy = new FileMonitorLogWriteStrategy(
    Path.of("logs", "monitor.log"), Duration.ofMinutes(10), 100_000,
    FileMonitorLogWriteStrategy.Format.READ_FRIENDLY)) {
  MonitorLogWriter writer = new MonitorLogWriter(
      new MonitorQueue(), strategy, BatchPolicy.fixedSize(1_000),
      FlushPolicy.after(Duration.ofSeconds(1)));
  Thread writerThread = new Thread(writer, "moniq-writer");
  writerThread.start();
  try {
    writer.submit(log);
    // Call from a user-input handler (for example, a console command or UI button).
    writer.flushAndRun(strategy.rollOutAction());
  } finally {
    writer.gracefulShutdown();
    writerThread.join();
  }
} // Close the strategy after the writer stops to release the file.
```

Choose one of two formats. `READ_FRIENDLY` is the default and writes labeled values, one log per
line, without a separate header:

```text
RequestType: produce, Id: message-1, Timestamp: 1000, TimestampNano: 2000, State: requested
```

`COMMA_SEPARATED` writes a header in every `.log` file followed by comma-separated records for easy
CSV conversion:

```text
RequestType,Id,Timestamp,TimestampNano,State
produce,message-1,1000,2000,requested
```

Comma-separated output quotes fields containing commas, quotes, or line breaks, preserving their
values when read by a CSV parser. Human-readable output escapes backslashes and line breaks so each
record stays on one line. The file extension comes from the supplied path for either format.

The strategy rolls out before writing the next log when either the current file is at least ten
minutes old or already contains 100,000 logs. Time is measured from the first write to each file
using a monotonic clock; idle periods do not create files. `Duration.ZERO` disables the time limit,
and `0` disables the count limit. The constructor taking only a `Path` enables manual roll-out only.

`rollOutAction()` returns the action that flushes and closes the current file; the next `write()`
creates the next file. Always pass it to `writer.flushAndRun(...)` rather than executing it directly.
The action's FIFO boundary applies to writes, not submissions, so logs already queued after the
boundary remain for the new file. Repeated requests without intervening writes do not create empty
files.

Files are named `monitor.log`, `monitor.1.log`, `monitor.2.log`, and so on. Existing files are skipped
without overwriting, and missing parent directories are created automatically. Headers in
comma-separated output do not count toward the limit. Use the same column layout for all logs sent
to one strategy when using comma-separated output.

`commit()` flushes buffered output and returns `false` on an I/O failure. Write, roll-out, and close
failures throw `UncheckedIOException`. The strategy implements `AutoCloseable`; `MonitorLogWriter`
flushes it on shutdown but does not close it, so the caller owns its lifetime.
