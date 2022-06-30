package com.ais.ti.ctx.raw.func.writer;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.Serializable;

/**
 * An implementation of {@code Writer} writing to the bucket files.
 *
 * @param <T> The type of the elements that are being written by the sink.
 */
public interface IWriter<T> extends Serializable {
    /**
     * Initializes the {@code Writer} for a newly opened bucket file. Any internal per-bucket
     * initialization should be performed here.
     *
     * @param fs   The {@link org.apache.hadoop.fs.FileSystem} containing the newly opened file.
     * @param path The {@link org.apache.hadoop.fs.Path} of the newly opened file.
     */
    void open(FileSystem fs, Path path) throws IOException;

    /**
     * @param fs   The {@link org.apache.hadoop.fs.FileSystem} containing the newly opened file.
     * @param path The {@link org.apache.hadoop.fs.Path} of the newly opened file.
     * @throws IOException if err.
     */
    void reOpen(FileSystem fs, Path path) throws IOException;

    /**
     * Flushes out any internally held data, and returns the offset that the file must be truncated to
     * at recovery.
     */
    long flush() throws IOException;

    /**
     * Retrieves the current position, and thus size, of the output file.
     */
    long getPos() throws IOException;

    /**
     * Closes the {@code Writer}. If the writer is already closed, no action will be taken. The call
     * should close all state related to the current output file, including the output stream opened
     * in {@code open}.
     */
    void close() throws IOException;

    /**
     * Writes one element to the bucket file.
     */
    void write(T element) throws IOException;

    /**
     * Duplicates the {@code Writer}. This is used to get one {@code Writer} for each parallel
     * instance of the sink.
     */
    IWriter<T> duplicate();
}
