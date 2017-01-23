package org.bitfunnel.runner;

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.SimpleCollector;

import java.util.ArrayList;

// Return Lucene IDs of all matches.

final class MatchingCollector extends SimpleCollector {
   private LeafReader currentReader;
   private final ArrayList<Integer> docIds;

   public MatchingCollector() {
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