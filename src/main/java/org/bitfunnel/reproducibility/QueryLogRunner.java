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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;


public class QueryLogRunner
{
    private final List<String> queries;
    private int[] matchCounts;
    private long[] timesInNS;
    private boolean[] succeeded;

    private final Index text;
    private final Index title;
    private final Object2ReferenceOpenHashMap<String,Index> indexMap;
    private final Object2ReferenceOpenHashMap<String, TermProcessor> termProcessors;

    ThreadSynchronizer warmupSynchronizer;
    AtomicInteger warmupQueriesRemaining = new AtomicInteger();

    ThreadSynchronizer performanceSynchronizer;
    AtomicInteger performanceQueriesRemaining = new AtomicInteger();

    ThreadSynchronizer finishSynchronizer;

    AtomicBoolean queriesFailed = new AtomicBoolean();

    AtomicLong firstStartTimeNs = new AtomicLong(Long.MAX_VALUE);
    AtomicLong lastFinishTimeNs = new AtomicLong(Long.MIN_VALUE);


    ArrayList<Thread> threads = new ArrayList<>(16);


    public QueryLogRunner(String basename, String queryLogFile) throws IllegalAccessException, InvocationTargetException, NoSuchMethodException, IOException, InstantiationException, URISyntaxException, ConfigurationException, ClassNotFoundException {
        // Load the query log.
        queries = LoadQueries(Paths.get(queryLogFile));
        matchCounts = new int[queries.size()];
        timesInNS = new long[queries.size()];
        succeeded = new boolean[queries.size()];

        // Load and configure the index.
        text = Index.getInstance( basename + "-text", true, true );
        title = Index.getInstance( basename + "-title", true, true );
        indexMap = new Object2ReferenceOpenHashMap<String,Index>(
                new String[] { "text", "title" }, new Index[] { text, title } );
        termProcessors = new Object2ReferenceOpenHashMap<String,TermProcessor>(
                new String[] { "text", "title" },
                new TermProcessor[] { text.termProcessor, title.termProcessor } );;
    }


    void go(int threadCount) throws InterruptedException {
        // Clear out any values from an earlier run.
        for (int i = 0; i < queries.size(); ++i) {
            matchCounts[i] = 0;
            timesInNS[i] = 0;
            succeeded[i] = false;
        }
        queriesFailed.set(false);

        // Set the number of queries for warmup and the test.
        // DESIGN NOTE: never use a value greater than queries.size().
        // For more information, see note in QueryProcessorThread.processLog().
        warmupQueriesRemaining.set(queries.size());
        performanceQueriesRemaining.set(queries.size());
        queriesFailed.set(false);

        warmupSynchronizer = new ThreadSynchronizer(threadCount);
        performanceSynchronizer = new ThreadSynchronizer(threadCount);

        finishSynchronizer = new ThreadSynchronizer(threadCount);

        long startTimeNs = System.nanoTime();

        System.out.println("Starting threads . . .");
        for (int i = 0; i < threadCount; ++i) {
            System.out.println(String.format("  thread-%d", i));
            Thread thread = new Thread(new QueryProcessorThread(), String.format("thread-%d", i));
            threads.add(thread);
            thread.start();
        }

        // Wait for last thread to exit.
        System.out.println("Waiting for threads . . .");
        for(int i = 0; i < threads.size(); i++)
            threads.get(i).join();

        long finishTimeNs = System.nanoTime();

        // TODO: write results to a file instead of the console.
        int failedQueriesCount = 0;
        for (int i = 0; i < queries.size(); ++i) {
            if (succeeded[i]) {
                System.out.println(
                        String.format("%s,%d,%f",
                                queries.get(i),
                                matchCounts[i],
                                timesInNS[i] * 1e-9));
            } else {
                ++failedQueriesCount;
                System.out.println(
                        String.format("%s,FAILED,FAILED", queries.get(i)));
            }
        }

        if (queriesFailed.get()) {
            System.out.println(String.format("WARNING: %d queries failed to execute.",
                                             failedQueriesCount));
        }

        // DESIGN NOTE: Measuring elapsed time three different ways to get a better understanding
        // of how to measure the query processing time.
        double elapsedTime = (lastFinishTimeNs.get() - firstStartTimeNs.get()) * 1e-9;
        double elapsedTime2 = (finishSynchronizer.startTimeNs - performanceSynchronizer.startTimeNs) * 1e-9;
        double elapsedTime3 = (finishTimeNs - startTimeNs) * 1e-9;

        System.out.println();
        System.out.println("====================================================");
        System.out.println();
        System.out.println(String.format("Thread count: %d", threadCount));
        System.out.println(String.format("Query count: %d", queries.size()));
        System.out.println(String.format("Total time: %f", elapsedTime));
        System.out.println(String.format("Total time (synchronizer): %f", elapsedTime2));
        System.out.println(String.format("Total time (simple): %f", elapsedTime3));
        System.out.println(String.format("QPS: %f", queries.size() / elapsedTime2));
    }


    // TODO: add command-line argument for thread count.
    // TODO: use SimpleJSAP argument parser here.
    public static void main( String arg[] ) throws Exception {
        QueryLogRunner runner = new QueryLogRunner(arg[0], arg[1]);
        runner.go(8);
    }


    public static List<String> LoadQueries(Path path) throws IOException {
        ArrayList<String> list = new ArrayList<String>();

        // DESIGN NOTE: For some reason, Files.lines() leads to the following exception
        // when attempting to read 06.efficiency_topics.all:
        //   java.nio.charset.MalformedInputException: Input length = 1
        // Using slightly more complex code based on FileReader to avoid the exception.

        File file = path.toFile();
        FileReader fileReader = new FileReader(file);
        BufferedReader bufferedReader = new BufferedReader(fileReader);
        String line;
        while ((line = bufferedReader.readLine()) != null) {
            list.add(line);
        }
        fileReader.close();

        return list;
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
            try {
                warmupSynchronizer.waitForAllThreadsReady();
                processLog(warmupQueriesRemaining);

                // Record performance measurements on final run.
                performanceSynchronizer.waitForAllThreadsReady();
                accumulateMin(firstStartTimeNs, System.nanoTime());
                processLog(performanceQueriesRemaining);
                accumulateMax(lastFinishTimeNs, System.nanoTime());

                finishSynchronizer.waitForAllThreadsReady();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }


        public void processLog(AtomicInteger queriesRemaining)
        {
            while (true) {
                int query = queriesRemaining.decrementAndGet();
                if (query < 0) {
                    break;
                }

                int queryIndex = queries.size() - (query % queries.size()) - 1;

                try {
                    long start = System.nanoTime();
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


        private void accumulateMax(AtomicLong accumulator, long newValue) {
            while (true) {
                long currentValue = accumulator.get();
                if (currentValue >= newValue) {
                    break;
                }

                if (accumulator.compareAndSet(currentValue, newValue)) {
                    break;
                }
            }
        }


        private void accumulateMin(AtomicLong accumulator, long newValue) {
            while (true) {
                long currentValue = accumulator.get();
                if (currentValue <= newValue) {
                    break;
                }

                if (accumulator.compareAndSet(currentValue, newValue)) {
                    break;
                }
            }
        }
    }
}
