package moniq.writer.strategy;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public abstract class OutputStreamBaseWriteStrategy implements IMonitorLogWriteStrategy {

    private final OutputStream outputStream;

    private final Charset charset;

    public OutputStreamBaseWriteStrategy(OutputStream outputStream, Charset charset) {
        this.outputStream = outputStream;
        this.charset = charset;
    }

    public OutputStreamBaseWriteStrategy(OutputStream outputStream) {
        this(outputStream, StandardCharsets.UTF_8);
    }

    protected void writeln(String line) {
        try {
            byte[] lineBytes = (line + "\n").getBytes(charset);
            outputStream.write(lineBytes);
        } catch (IOException e) {
            throw new RuntimeException(e); // TODO: handle this exception properly
        }
    }

    @Override
    public boolean commit() {
        try {
            outputStream.flush();
        } catch (IOException e) {
            return false; // TODO: handle this exception properly
        }
        return true;
    }
}
