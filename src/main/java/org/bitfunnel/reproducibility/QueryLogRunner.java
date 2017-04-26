package org.bitfunnel.reproducibility;


import it.unimi.di.big.mg4j.index.Index;
import it.unimi.di.big.mg4j.index.TermProcessor;
import it.unimi.di.big.mg4j.query.SelectedInterval;
import it.unimi.di.big.mg4j.query.nodes.QueryBuilderVisitorException;
import it.unimi.di.big.mg4j.query.parser.QueryParserException;
import it.unimi.di.big.mg4j.query.parser.SimpleParser;
import it.unimi.di.big.mg4j.search.DocumentIteratorBuilderVisitor;
import it.unimi.di.big.mg4j.search.score.DocumentScoreInfo;
import it.unimi.dsi.fastutil.objects.Object2ReferenceOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.Reference2ObjectMap;
import org.apache.commons.configuration.ConfigurationException;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class QueryLogRunner
{
    private final List<String> queries;
    private final Index text;
    private final Index title;
    private final Object2ReferenceOpenHashMap<String,Index> indexMap;
    private final Object2ReferenceOpenHashMap<String, TermProcessor> termProcessors;

    private int[] matchCounts;
    private long[] timesInNS;
    private boolean[] succeeded;

    ThreadSynchronizer warmupSynchronizer;
    AtomicInteger warmupQueriesRemaining;

    ThreadSynchronizer performanceSynchronizer;
    AtomicInteger performanceQueriesRemaining;

    AtomicBoolean queriesFailed;

    ArrayList<Thread> threads;


    public QueryLogRunner(String basename, String queryLogFile) throws IllegalAccessException, InvocationTargetException, NoSuchMethodException, IOException, InstantiationException, URISyntaxException, ConfigurationException, ClassNotFoundException {
        // Load the query log.
        queries = ReadLines(Paths.get(queryLogFile));

        // Load and configure the index.
        text = Index.getInstance( basename + "-text", true, true );
        title = Index.getInstance( basename + "-title", true, true );
        indexMap = new Object2ReferenceOpenHashMap<String,Index>(
                new String[] { "text", "title" }, new Index[] { text, title } );
        termProcessors = new Object2ReferenceOpenHashMap<String,TermProcessor>(
                new String[] { "text", "title" },
                new TermProcessor[] { text.termProcessor, title.termProcessor } );;

        queriesFailed.set(false);
    }


    void go(int threadCount) {
        // Clear out any values from an earlier run.
        for (int i = 0; i < queries.size(); ++i) {
            matchCounts[i] = 0;
            timesInNS[i] = 0;
            succeeded[i] = false;
        }

        // Set the number of queries for warmup and the test.
        // DESIGN NOTE: never use a value greater than queries.size().
        // For more information, see note in QueryProcessorThread.processLog().
        warmupQueriesRemaining.set(queries.size());
        performanceQueriesRemaining.set(queries.size());
        queriesFailed.set(false);

        warmupSynchronizer = new ThreadSynchronizer(threadCount);
        performanceSynchronizer = new ThreadSynchronizer(threadCount);

        for (int i = 0; i < threadCount; ++i) {
            Thread thread = new Thread(new QueryProcessorThread(), String.format("thread-%d", i));
            threads.add(thread);
            thread.start();
        }

        // TODO: wait for last thread to exit.
        // TODO: write out results to a file instead of the console.
        for (int i = 0; i < queries.size(); ++i) {
            if (succeeded[i]) {
                System.out.println(
                        String.format("%s,%d,%f",
                                queries.get(i),
                                matchCounts[i],
                                timesInNS[i] * 1e-9));
            } else {
                System.out.println(
                        String.format("%s,FAILED,FAILED", queries.get(i)));
            }
        }
    }


    public static List<String> ReadLines(Path file) throws IOException {
        List<String> list = null;
        Stream<String> lines = Files.lines(file);
        return lines.collect(Collectors.toList());
    }


    private class QueryProcessorThread implements Runnable
    {
        private final ExperimentalQueryEngine engine;
        ObjectArrayList<DocumentScoreInfo<Reference2ObjectMap<Index, SelectedInterval[]>>> results;

        private QueryProcessorThread() {
            engine = new ExperimentalQueryEngine(
                new SimpleParser( indexMap.keySet(), "text", termProcessors ),
                new DocumentIteratorBuilderVisitor( indexMap, text, 1000 ),
                indexMap);

            results =
                new ObjectArrayList<DocumentScoreInfo<Reference2ObjectMap<Index,SelectedInterval[]>>>();
        }

        @Override
        public void run() {
            // Process all queries once to "warm up the system".
            warmupSynchronizer.waitForAllThreadsReady();
            processLog(warmupQueriesRemaining);

            // Record performance measurements on final run.
            performanceSynchronizer.waitForAllThreadsReady();
            processLog(performanceQueriesRemaining);
        }


        public void processLog(AtomicInteger queriesRemaining)
        {
            while (true) {
                int query = queriesRemaining.decrementAndGet();
                if (query < 0) {
                    break;
                }

                int queryIndex = query % queries.size();
                long start = System.nanoTime();

                try {
                    results.clear();
                    engine.process(queries.get(queryIndex), 0, 1000000000, results);

                    // DESIGN NOTE: These writes are safe in a multi-threaded environment as long
                    // as two threads never have the same queryIndex. One can guarantee this by
                    // restricting queriesRemaining to values that don't exceed queries.size().
                    // If queriesRemaining is larger, the modulus operation used to compute QueryIndex
                    // could lead to two threads getting assigned the same queryIndex.
                    timesInNS[queryIndex] = System.nanoTime() - start;
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
    }
}
