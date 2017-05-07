package org.bitfunnel.reproducibility;

import it.unimi.di.big.mg4j.index.Index;
import it.unimi.di.big.mg4j.query.SelectedInterval;
import it.unimi.di.big.mg4j.query.nodes.QueryBuilderVisitorException;
import it.unimi.di.big.mg4j.query.parser.QueryParserException;
import it.unimi.di.big.mg4j.query.parser.SimpleParser;
import it.unimi.di.big.mg4j.search.DocumentIteratorBuilderVisitor;
import it.unimi.di.big.mg4j.search.score.DocumentScoreInfo;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.Reference2ObjectMap;

import java.io.IOException;


class Mg4jQueryProcessor extends QueryProcessorBase
{
    private final ExperimentalQueryEngine engine;
    ObjectArrayList<DocumentScoreInfo<Reference2ObjectMap<Index, SelectedInterval[]>>> results;

    Mg4jQueryProcessor(Mg4jIndex index, QueryLogRunner runner)
    {
        super(runner);

        engine = new ExperimentalQueryEngine(
            new SimpleParser( index.indexMap.keySet(), "text", index.termProcessors ),
            new DocumentIteratorBuilderVisitor(
                    index.indexMap,
                    index.text, 1000 ),
            index.indexMap);

        results =
            new ObjectArrayList<DocumentScoreInfo<Reference2ObjectMap<Index,SelectedInterval[]>>>();
    }


    @Override public void processOneQuery(int queryIndex, String query) {
        try {
            long start = System.nanoTime();

            //
            // Run query
            //

            results.clear();
            engine.process(query, 0, 1000000000, results);

            // DESIGN NOTE: These writes are safe in a multi-threaded environment as long
            // as two threads never have the same queryIndex. One can guarantee this by
            // restricting queriesRemaining to values that don't exceed queries.size().
            // If queriesRemaining is larger, the modulus operation used to compute QueryIndex
            // could lead to two threads getting assigned the same queryIndex.
            matchTimesInNS[queryIndex] = System.nanoTime() - start;
            matchCounts[queryIndex] = results.size();
            succeeded[queryIndex] = true;
        } catch (QueryParserException e) {
            succeeded[queryIndex] = false;
            e.printStackTrace();
        } catch (QueryBuilderVisitorException e) {
            succeeded[queryIndex] = false;
            e.printStackTrace();
        } catch (IOException e) {
            succeeded[queryIndex] = false;
            e.printStackTrace();
        }
    }
}

