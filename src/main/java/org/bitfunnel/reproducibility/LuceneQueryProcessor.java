package org.bitfunnel.reproducibility;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;

import java.io.IOException;


public class LuceneQueryProcessor extends QueryProcessorBase
{
    LuceneIndex index;
    LuceneCollector collector = new LuceneCollector();
    DirectoryReader reader;
    IndexSearcher searcher;


    LuceneQueryProcessor(LuceneIndex index, QueryLogRunner runner) throws IOException {
        super(runner);

        this.index = index;

        reader = DirectoryReader.open(index.dir);

        System.out.println(String.format("Document count: %d", reader.numDocs()));

        // https://lucene.apache.org/core/4_2_1/core/org/apache/lucene/index/package-summary.html#stats
        // http://stackoverflow.com/questions/31327126/accessing-terms-statistics-in-lucene-4
        System.out.println(String.format("getSumDocFreq(): %d", reader.getSumDocFreq("00")));
        System.out.println(String.format("getSumTotalTermFreq(): %d", reader.getSumTotalTermFreq("00")));

        searcher = new IndexSearcher(reader);
    }


    @Override public void processOneQuery(int queryIndex, String queryText)
    {
        try {
            long start = System.nanoTime();

            //
            // Run query
            //

            collector.getDocIds().clear();
            String[] terms = queryText.split(" ");
            // Note: we also tried using Lucene's "Classic" query parser.
            // It gets the same results but it's slightly slower,
            // presumebly due to the extra overhead of having to actually parse the string.
            BooleanQuery.Builder queryBuilder = new BooleanQuery.Builder();
            for (String termText : terms) {
                // TODO: The commend below seems wrong. See issue #32.
                // field seems to be "00" for title, "01" for body.
                Term term = new Term("00", termText);
                TermQuery termQuery = new TermQuery(term);
                // Using FILTER instead of MUST because FILTER doesn't score.
                queryBuilder.add(termQuery, BooleanClause.Occur.FILTER);
            }
            BooleanQuery tempQuery = queryBuilder.build();
            ConstantScoreQuery query = new ConstantScoreQuery(tempQuery);

            long matchingStart = System.nanoTime();

            searcher.search(query, collector);

            long finishTime = System.nanoTime();

            // DESIGN NOTE: These writes are safe in a multi-threaded environment as long
            // as two threads never have the same queryIndex. One can guarantee this by
            // restricting queriesRemaining to values that don't exceed queries.size().
            // If queriesRemaining is larger, the modulus operation used to compute QueryIndex
            // could lead to two threads getting assigned the same queryIndex.
            planningTimesInNS[queryIndex] = matchingStart - start;
            matchTimesInNS[queryIndex] = finishTime - matchingStart;
            matchCounts[queryIndex] = collector.getDocIds().size();
            succeeded[queryIndex] = true;
        }
        catch (IOException e) {
            succeeded[queryIndex] = false;
            e.printStackTrace();
        }
    }
}
