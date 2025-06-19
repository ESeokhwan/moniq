package moniq.writer.strategy;

import moniq.IMonitorLog;
import org.jetbrains.annotations.Nullable;

import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class ReadFriendlyWriteStrategy extends OutputStreamBaseWriteStrategy {

    private final String delimiter;

    private final IHeaderValueConcator concator;

    private final ILineWrapper lineWrapper;

    public ReadFriendlyWriteStrategy(OutputStream outputStream, Charset charset, String delimiter, IHeaderValueConcator concator, ILineWrapper lineWrapper) {
        super(outputStream, charset);
        this.delimiter = delimiter;
        this.concator = concator;
        this.lineWrapper = lineWrapper;
    }

    public ReadFriendlyWriteStrategy(OutputStream outputStream, String delimiter, IHeaderValueConcator concator, ILineWrapper lineWrapper) {
        this(outputStream, StandardCharsets.UTF_8, delimiter, concator, lineWrapper);
    }

    public ReadFriendlyWriteStrategy(OutputStream outputStream) {
        this(outputStream, StandardCharsets.UTF_8, ", ", (header, value) -> header + ": " + value, (line) -> line);
    }

    @Override
    public void write(IMonitorLog log) {
        List<String> headers = log.getHeaders();
        List<String> values = log.getValues();

        List<String> valueWithHeaders = new ArrayList<>();
        for (int i = 0; i < values.size(); i++) {
            valueWithHeaders.add(concator.concat(
                i < headers.size() ? headers.get(i) : null,
                log.getValues().get(i)
            ));
        }
        writeln(lineWrapper.wrap(String.join(delimiter, valueWithHeaders)));
    }

    public interface IHeaderValueConcator {
        String concat(@Nullable String header, String value);
    }

    public interface ILineWrapper {
        String wrap(String line);
    }
}
