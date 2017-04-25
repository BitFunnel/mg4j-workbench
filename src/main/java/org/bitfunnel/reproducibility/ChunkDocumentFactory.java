package org.bitfunnel.reproducibility;

import it.unimi.di.big.mg4j.document.Document;
import it.unimi.di.big.mg4j.document.DocumentFactory;
import it.unimi.dsi.fastutil.objects.Reference2ObjectMap;

import java.io.IOException;
import java.io.InputStream;
import java.io.PushbackInputStream;

public class ChunkDocumentFactory implements DocumentFactory {
    @Override
    public int numberOfFields() {
        return 2;
    }

    @Override
    public String fieldName(int i) {
        // TODO: replace hard-coded field names with data-driven solution.
        if (i == 0) {
            return "text";
        }
        else if (i == 1) {
            return "title";
        }
        else {
            return null;
        }
    }

    @Override
    public int fieldIndex(String s) {
        // TODO: replace hard-coded field names with data-driven solution.
        if (s.equals("text"))
        {
            return 0;
        }
        else if (s.equals("title")) {
            return 1;
        }
        else
        {
            return -1;
        }
    }

    @Override
    public FieldType fieldType(int i) {
        // TODO: replace hard-coded field names with data-driven solution.
        if (i == 0 || i == 1) {
            return DocumentFactory.FieldType.TEXT;
        }
        else {
            return null;
        }
    }

    @Override
    public Document getDocument(InputStream inputStream, Reference2ObjectMap<Enum<?>, Object> reference2ObjectMap) throws IOException {
        System.out.println("ChunkDocumentFactory.getDocument()");
        return new ChunkDocument(new PushbackInputStream(inputStream));
    }

    @Override
    public DocumentFactory copy() {
        System.out.println("ChunkDocumentFactory.copy(): not implemented - returns null.");
        return null;
    }
}
