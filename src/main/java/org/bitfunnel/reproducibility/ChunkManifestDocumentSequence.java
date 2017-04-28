package org.bitfunnel.reproducibility;

import it.unimi.di.big.mg4j.document.DocumentFactory;
import it.unimi.di.big.mg4j.document.DocumentIterator;
import it.unimi.di.big.mg4j.document.DocumentSequence;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;

public class ChunkManifestDocumentSequence implements DocumentSequence{
    // List of chunk files read from manifest file.
    private String[] files;

    public ChunkManifestDocumentSequence(String manifest) throws IOException {
        files = Files.readAllLines(Paths.get(manifest), Charset.defaultCharset()).toArray(new String[]{});
    }

    @Override
    public DocumentIterator iterator() throws IOException {
        return new ChunkManifestDocumentIterator(files);
    }

    @Override
    public DocumentFactory factory() {
        return new ChunkDocumentFactory();
    }

    @Override
    public void close() throws IOException {
        // Intentional nop.
    }

    @Override
    public void filename(CharSequence charSequence) throws IOException {
        // Intentional nop. See javadoc for filename() method of it.unimi.di.big.mg4j.document.DocumentSequence
        // for more information.
    }
}
