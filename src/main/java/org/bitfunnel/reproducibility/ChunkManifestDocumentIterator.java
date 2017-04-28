package org.bitfunnel.reproducibility;

import it.unimi.di.big.mg4j.document.Document;
import it.unimi.di.big.mg4j.document.DocumentIterator;
import it.unimi.di.big.mg4j.document.DocumentSequence;

import java.io.IOException;

public class ChunkManifestDocumentIterator implements DocumentIterator {
    private String[] chunkFiles;
    private int current = 0;
    private DocumentSequence sequence = null;
    private DocumentIterator iterator = null;
    private Document document = null;

    public ChunkManifestDocumentIterator(String[] files) throws IOException {
        chunkFiles = files;
    }

    @Override
    public Document nextDocument() throws IOException {
        // If there was a document from previous call, close it.
        if (document != null) {
            document.close();
            document = null;
        }

        // If we haven't finished processing all of the chunks,
        // attempt to get the next document.
        while (true) {
            // If we have an iterator from a previous call, see if it has another document.
            if (iterator != null) {
                document = iterator.nextDocument();

                // If it does, return it.
                if (document != null)
                {
                    return document;
                }

                // Otherwise, this iterator is used up, so close it along with its sequence.
                iterator.close();
                sequence.close();
            }

            // If we got here, the iterator was either null because we're on the first call,
            // or we just closed an empty iterator. Either way, open up the next iterator and
            // go back to the top of the loop.
            if (current < chunkFiles.length) {
                sequence = new ChunkDocumentSequence(chunkFiles[current++]);
                iterator = sequence.iterator();
            }
            else {
                break;
            }
        }

        // If we got here, we've exhausted all of the iterators. If there is an iterator
        // that is still open, close it.
        if (current == chunkFiles.length && iterator != null){
            iterator.close();
            iterator = null;
            sequence.close();
            sequence = null;
        }

        // Return null to indicate that we've reached the end of the sequence of chunks.
        return null;
    }

    @Override
    public void close() throws IOException {
        if (document != null) {
            document.close();
            document = null;
        }
        if (iterator != null) {
            iterator.close();
            iterator = null;
        }
        if (sequence != null) {
            sequence.close();
            sequence = null;
        }
    }
}
