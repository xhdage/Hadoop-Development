package com.ais.ti.ctx.raw.func.writer;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public abstract class FsWriterBase<T> implements IWriter<T> {

    private static final long serialVersionUID = 3L;

    private static final int DEFAULT_BUFFER_SIZE = 1024 * 1024;

    /**
     * The {@code FSDataOutputStream} for the current part file.
     */
    private transient FSDataOutputStream outStream;

    private boolean syncOnFlush;

    private int bufferSize = DEFAULT_BUFFER_SIZE;

    public FsWriterBase() {
    }

    protected FsWriterBase(FsWriterBase<T> other, int bufferSize) {
        this.syncOnFlush = other.syncOnFlush;
        this.bufferSize = bufferSize;
    }

    /**
     * Controls whether to sync {@link FSDataOutputStream} on flush.
     */
    public void setSyncOnFlush(boolean syncOnFlush) {
        this.syncOnFlush = syncOnFlush;
    }

    /**
     * Returns the current output stream, if the stream is open.
     */
    protected FSDataOutputStream getStream() {
        if (outStream == null) {
            throw new IllegalStateException("Output stream has not been opened");
        }
        return outStream;
    }

    @Override
    public void open(FileSystem fs, Path path) throws IOException {
        if (outStream != null) {
            throw new IllegalStateException("Writer has already been opened");
        }
        outStream = fs.create(path, false);
    }

    @Override
    public void reOpen(FileSystem fs, Path path) throws IOException {
        if (outStream != null) {
            throw new IllegalStateException("Writer has already been opened");
        }
        boolean exists = fs.exists(path);
        if (!exists) {
            throw new IllegalStateException(String.format("File path is not exists:%s", path.toString()));
        }
        FileStatus fileLinkStatus = fs.getFileLinkStatus(path);
        if (!fileLinkStatus.isFile()) {
            throw new IllegalStateException(
                    String.format("This path '%s' is not a file path", path.toString()));
        }
        outStream = fs.append(path, bufferSize);
    }

    @Override
    public long flush() throws IOException {
        if (outStream == null) {
            throw new IllegalStateException("Writer is not open");
        }
        if (syncOnFlush) {
            outStream.hsync();
        } else {
            outStream.hflush();
        }
        return outStream.getPos();
    }

    @Override
    public long getPos() throws IOException {
        if (outStream == null) {
            throw new IllegalStateException("Writer is not open");
        }
        return outStream.getPos();
    }

    @Override
    public void close() throws IOException {
        if (outStream != null) {
            flush();
            outStream.close();
            outStream = null;
        }
    }

    public int getBufferSize() {
        return bufferSize;
    }

    public void setBufferSize(int bufferSize) {
        this.bufferSize = bufferSize;
    }

    public boolean isSyncOnFlush() {
        return syncOnFlush;
    }
}
