package org.bitfunnel.reproducibility;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

public class ChunkFile {

    OutputStream outputStream;

    public ChunkFile(OutputStream outputStream)
    {
        this.outputStream = outputStream;
    }


    public void emit(String text) {
        try {
            outputStream.write(text.getBytes(StandardCharsets.UTF_8));
            outputStream.write((byte)0);
        }
        catch (IOException e) {
            throw new RuntimeException("Error writing bytes.");
        }
    }


    public class FileScope implements java.lang.AutoCloseable {
        public FileScope() {
        }

        @Override
        public void close() throws Exception {
            // Write trailing '\0'
            emit("");
        }
    }


    public class DocumentScope implements java.lang.AutoCloseable {
        public DocumentScope(int documentId) {
            emit(String.format("%016x", documentId));
        }

        @Override
        public void close() throws Exception {
            // Write trailing '\0'
            emit("");
        }
    }


    public class StreamScope implements java.lang.AutoCloseable {
        public StreamScope(int streamId) {
            emit(String.format("%02x", streamId));
        }

        @Override
        public void close() throws Exception {
            // Write trailing '\0'
            emit("");
        }
    }
}
