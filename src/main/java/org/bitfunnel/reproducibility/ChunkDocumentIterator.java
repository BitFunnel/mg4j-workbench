package org.bitfunnel.reproducibility;

import it.unimi.di.big.mg4j.document.Document;

import java.io.IOException;
import java.io.InputStream;
import java.io.PushbackInputStream;

public class ChunkDocumentIterator implements it.unimi.di.big.mg4j.document.DocumentIterator {
    PushbackInputStream input;

    public ChunkDocumentIterator(InputStream input) {
        this.input = new PushbackInputStream(input);
    }


    @Override
    public Document nextDocument() throws IOException {
        System.out.println("ChunkDocumentSequence.nextDocument()");

        int c = input.read();
        if (c == -1) {
            throw new IOException("ChunkDocumentIterator.nextDocument(): unexpected EOF.");
        }
        else if (c == '\0') {
            // No more documents left.

            if (input.read() != -1) {
                throw new IOException("ChunkDocumentIterator.nextDocument(): expected EOF, but found trailing content.");
            }

            return null;
        }
        else {
            // There's at least one more document left.
            // Position the stream at the beginning of the document.
            input.unread(c);
            return new ChunkDocument(input);
        }
    }


    @Override
    public void close() throws IOException {
        System.out.println("ChunkDocumentIterator.close()");
    }
}
