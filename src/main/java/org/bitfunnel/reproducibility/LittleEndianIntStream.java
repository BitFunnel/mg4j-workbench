package org.bitfunnel.reproducibility;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;


public class LittleEndianIntStream {
    WritableByteChannel channel;

    // WARNING: Code assumes that BUFFER_SIZE is at least 4.
    static final int BUFFER_SIZE = 8192;
    ByteBuffer buffer;


    public LittleEndianIntStream(FileOutputStream out) {
        channel = out.getChannel();
        buffer = ByteBuffer.allocate(BUFFER_SIZE);
        buffer.order (ByteOrder.LITTLE_ENDIAN);
    }


    void putInt(int x) throws IOException {
        if (buffer.limit() - buffer.position() < 4) {
            drain();
        }
        buffer.putInt(x);
    }


    void drain() throws IOException {
        buffer.flip();
        channel.write(buffer);
        buffer.clear();
    }


    void close() throws IOException {
        if (buffer.position() > 0) {
            drain();
        }
    }
}
