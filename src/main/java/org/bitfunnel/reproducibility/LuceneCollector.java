package org.bitfunnel.reproducibility;

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.SimpleCollector;

import java.util.ArrayList;

// TODO: Consider moving this class inside of LuceneQueryProcessor.
final class LuceneCollector extends SimpleCollector {
    private LeafReader currentReader;
    private final ArrayList<Integer> docIds;

    public LuceneCollector() {
        this.docIds = new ArrayList<Integer>();
    }

    public ArrayList<Integer> getDocIds() {
        return docIds;
    }

    @Override public void collect(final int doc) {
        docIds.add(doc);
    }

    @Override protected void doSetNextReader (final LeafReaderContext context) {
        currentReader = context.reader();
    }

    @Override public boolean needsScores() {
        return false;
    }
}