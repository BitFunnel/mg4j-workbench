package org.bitfunnel.reproducibility;

import it.unimi.di.big.mg4j.document.DocumentFactory;
import it.unimi.di.big.mg4j.document.DocumentIterator;
import it.unimi.di.big.mg4j.document.DocumentSequence;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

public class ChunkDocumentSequence implements DocumentSequence{
    FileInputStream input;

    /**
     * Constructs a ChunkDocumentSequence corresponding to a BitFunnel chunk file.
     * See <a href="http://bitfunnel.org/corpus-file-format/">Corpus File Format.</a>
     * @param file
     * @throws FileNotFoundException
     */
    public ChunkDocumentSequence(String file) throws FileNotFoundException {
        input = new FileInputStream(file);
        System.out.println(String.format("ChunkDocumentSequence.ChunkDocumentSequence(%s)", file));
    }

    @Override
    public DocumentIterator iterator() throws IOException {
        System.out.println("ChunkDocumentSequence.iterator()");
        return new ChunkDocumentIterator(input);
    }

    @Override
    public DocumentFactory factory() {
        System.out.println("ChunkDocumentSequence.factory()");
        return new ChunkDocumentFactory();
    }

    @Override
    public void close() throws IOException {
        System.out.println("ChunkDocumentSequence.close()");
        input.close();
    }

    @Override
    public void filename(CharSequence charSequence) throws IOException {
        System.out.println(String.format("ChunkDocumentSequence.filename(%s)", charSequence));
        // Intentional nop. See javadoc for filename() method of it.unimi.di.big.mg4j.document.DocumentSequence
        // for more information.
    }
}
