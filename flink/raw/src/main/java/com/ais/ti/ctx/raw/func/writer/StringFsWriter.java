package com.ais.ti.ctx.raw.func.writer;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.IllegalCharsetNameException;
import java.nio.charset.UnsupportedCharsetException;

/**
 * A {@link IWriter} that uses {@code toString()} on the input elements and writes them to the
 * output bucket file separated by newline.
 *
 * @param <T> The type of the elements that are being written by the sink.
 */
@SuppressWarnings("DuplicatedCode")
public class StringFsWriter<T> extends FsWriterBase<T> {
    private static final long serialVersionUID = 1L;

    private String charsetName;

    private transient Charset charset;

    /**
     * Creates a new {@code StringFsWriter} that uses {@code "UTF-8"} charset to convert strings to
     * bytes.
     */
    public StringFsWriter() {
        this("UTF-8");
    }

    /**
     * Creates a new {@code StringFsWriter} that uses the given charset to convert strings to bytes.
     *
     * @param charsetName Name of the charset to be used, must be valid input for {@code
     *                    Charset.forName(charsetName)}
     */
    public StringFsWriter(String charsetName) {
        this.charsetName = charsetName;
    }

    protected StringFsWriter(StringFsWriter<T> other, int buffSize) {
        super(other, buffSize);
        this.charsetName = other.charsetName;
    }

    @Override
    public void open(FileSystem fs, Path path) throws IOException {
        super.open(fs, path);
        try {
            this.charset = Charset.forName(charsetName);
        } catch (IllegalCharsetNameException e) {
            throw new IOException("The charset " + charsetName + " is not valid.", e);
        } catch (UnsupportedCharsetException e) {
            throw new IOException("The charset " + charsetName + " is not supported.", e);
        }
    }

    @Override
    public void reOpen(FileSystem fs, Path path) throws IOException {
        super.reOpen(fs, path);
        try {
            this.charset = Charset.forName(charsetName);
        } catch (IllegalCharsetNameException e) {
            throw new IOException("The charset " + charsetName + " is not valid.", e);
        } catch (UnsupportedCharsetException e) {
            throw new IOException("The charset " + charsetName + " is not supported.", e);
        }
    }

    @Override
    public void write(T element) throws IOException {
        FSDataOutputStream outputStream = getStream();
        outputStream.write(element.toString().getBytes(charset));
        outputStream.write('\n');
    }

    @Override
    public StringFsWriter<T> duplicate() {
        return new StringFsWriter<>(this, getBufferSize());
    }

    String getCharsetName() {
        return charsetName;
    }
}
