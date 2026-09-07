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

On shutdown, the writer wakes automatically, stops accepting `submit()` calls, drains queued logs,
flushes the strategy, and terminates. The caller still owns the writer thread and must `join()` it.

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
- `ScrapableWriteStrategy` for delimiter-separated output with an optional header;
- `ReadFriendlyWriteStrategy` for labeled human-readable output;
- `NoOpWriteStrategy` for measuring monitoring overhead without output.

Custom exporters implement `IMonitorLogWriteStrategy`. `write()` receives preprocessed logs and
`commit()` is called at flush boundaries.
