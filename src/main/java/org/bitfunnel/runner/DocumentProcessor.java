package org.bitfunnel.runner;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;

import java.io.IOException;

public class DocumentProcessor implements IDocumentProcessor {
    IndexWriter writer;
    Document currentDocument;
    StringBuilder currentString;
    String currentStreamName;


    public DocumentProcessor(IndexWriter writer) {
        this.writer = writer;
        currentString = new StringBuilder();
        currentDocument = new Document();
    }

    @Override
    public void openDocumentSet() {
    }

    @Override
    public void openDocument(Long documentId) {
        currentDocument.clear();
        // System.out.println("openDocument " + documentId);
        Field field = new StoredField("id", documentId);
        currentDocument.add(field);
    }

    @Override
    public void openStream(String name) {
        // System.out.println("openStream: " + name);
        currentStreamName = name;
        currentString.setLength(0);
    }

    @Override
    public void term(String term) {
        // System.out.println("term: " + term);
        currentString.append(" " + term);
    }

    @Override
    public void closeStream() {
        // TODO: since we're not ranking, we should disable norms to save space.
        Field field = new Field(currentStreamName, currentString.toString(), TextField.TYPE_NOT_STORED);
        currentDocument.add(field);
    }

    @Override
    public void closeDocument() {
        try {
            writer.addDocument(currentDocument);
        } catch (IOException e) {
            e.printStackTrace();
            // TODO: throw?
            // Assert.fail();
        }

    }

    @Override
    public void closeDocumentSet() {

        //outputStream.write(0);
    }
}
