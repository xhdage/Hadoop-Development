package com.ais.ti.ctx.raw.func.writer;

import com.ais.ti.ctx.raw.entities.SinkPathData;
import org.apache.hadoop.fs.FSDataOutputStream;

import java.io.IOException;

public class JsonWriter extends StringFsWriter<SinkPathData> {

    public JsonWriter() {
        super();
    }

    public JsonWriter(JsonWriter jsonWriter, int bufferSize) {
        super(jsonWriter, bufferSize);
    }

    @Override
    public void write(SinkPathData element) throws IOException {
        FSDataOutputStream outputStream = getStream();
        outputStream.write(element.getData().getBytes(getCharsetName()));
        outputStream.write('\n');
    }

    @Override
    public JsonWriter duplicate() {
        return new JsonWriter(this, getBufferSize());
    }
}
