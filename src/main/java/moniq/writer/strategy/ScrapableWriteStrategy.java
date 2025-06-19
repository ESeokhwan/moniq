package moniq.writer.strategy;

import moniq.IMonitorLog;

import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class ScrapableWriteStrategy extends OutputStreamBaseWriteStrategy {

    private final String delimiter;

    private final boolean needHeader;

    private boolean isInitialized = false;

    public ScrapableWriteStrategy(OutputStream outputStream, Charset charset, String delimiter, boolean needHeader) {
        super(outputStream, charset);
        this.delimiter = delimiter;
        this.needHeader = needHeader;
    }

    public ScrapableWriteStrategy(OutputStream outputStream, String delimiter, boolean needHeader) {
        this(outputStream, StandardCharsets.UTF_8, delimiter, needHeader);
    }

    public ScrapableWriteStrategy(OutputStream outputStream) {
        this(outputStream, StandardCharsets.UTF_8, ", ", true);
    }

    @Override
    public void write(IMonitorLog log) {
        if (needHeader && !isInitialized) {
            writeln(String.join(delimiter, log.getHeaders()));
            isInitialized = true;
        }
        writeln(String.join(delimiter, log.getValues()));
    }
}
