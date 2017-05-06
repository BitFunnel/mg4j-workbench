package org.bitfunnel.reproducibility;

import com.martiansoftware.jsap.*;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.io.output.FileWriterWithEncoding;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;


public class QueryLogRunner
{
    final List<String> queries;
    int[] matchCounts;
    long[] timesInNS;
    boolean[] succeeded;

    QueryProcessorFactory factory;

    ThreadSynchronizer warmupSynchronizer;
    AtomicInteger warmupQueriesRemaining = new AtomicInteger();

    ThreadSynchronizer performanceSynchronizer;
    AtomicInteger performanceQueriesRemaining = new AtomicInteger();

    ThreadSynchronizer finishSynchronizer;

    ArrayList<Thread> threads = new ArrayList<>(16);


    public QueryLogRunner(QueryProcessorFactory factory, String queryLogFile) throws IllegalAccessException, InvocationTargetException, NoSuchMethodException, IOException, InstantiationException, URISyntaxException, ConfigurationException, ClassNotFoundException {
        // Load the query log.
        this.factory = factory;
        queries = Utilities.LoadQueries(Paths.get(queryLogFile));
        matchCounts = new int[queries.size()];
        timesInNS = new long[queries.size()];
        succeeded = new boolean[queries.size()];
    }


    void go(int threadCount, Path outfilePath) throws InterruptedException, IOException, IllegalAccessException, InvocationTargetException, InstantiationException, ConfigurationException, URISyntaxException, NoSuchMethodException, ClassNotFoundException {
        // Clear out any values from an earlier run.
        for (int i = 0; i < queries.size(); ++i) {
            matchCounts[i] = 0;
            timesInNS[i] = 0;
            succeeded[i] = false;
        }

        // Set the number of queries for warmup and the test.
        // WARNING: never use a value greater than queries.size().
        // For rationale, see DESIGN NOTE in Mg4jQueryProcessor.processLog()
        // and LuceneQueryProcessor.processLog().
        warmupQueriesRemaining.set(queries.size());
        performanceQueriesRemaining.set(queries.size());

        warmupSynchronizer = new ThreadSynchronizer(threadCount);
        performanceSynchronizer = new ThreadSynchronizer(threadCount);

        finishSynchronizer = new ThreadSynchronizer(threadCount);

        System.out.println(String.format("Starting %d threads . . .", threadCount));
        for (int i = 0; i < threadCount; ++i) {
            System.out.println(String.format("  thread-%d", i));
            Thread thread = new Thread(
                    factory.createQueryProcessor(this),
                    String.format("thread-%d", i));
            threads.add(thread);
            thread.start();
        }

        // Wait for last thread to exit.
        System.out.println("Waiting for threads to exit . . .");
        for(int i = 0; i < threads.size(); i++)
            threads.get(i).join();

        System.out.println(String.format("Writing results to \"%s\".", outfilePath));

        int failedQueriesCount = 0;
        int processedCount = 0;
        double totalLatency = 0;
        File outFile = outfilePath.toFile();
        BufferedWriter writer =  null;
        try {
            writer = new BufferedWriter(new FileWriterWithEncoding(outFile, StandardCharsets.UTF_8));

            for (int i = 0; i < queries.size(); ++i) {
                if (succeeded[i]) {
                    ++processedCount;
                    totalLatency += (timesInNS[i] * 1e-9);
                    writer.write(
                            String.format("%s,%d,%f\n",
                                    queries.get(i),
                                    matchCounts[i],
                                    timesInNS[i] * 1e-9));
                } else {
                    ++failedQueriesCount;
                    writer.write(
                            String.format("%s,FAILED,FAILED\n", queries.get(i)));
                }
            }
        }
        finally {
            if (writer != null) {
                writer.close();
            }
        }

        if (failedQueriesCount > 0) {
            System.out.println(String.format("WARNING: %d queries failed to execute.",
                                             failedQueriesCount));
        }

        double elapsedTime = (finishSynchronizer.startTimeNs - performanceSynchronizer.startTimeNs) * 1e-9;
        System.out.println();
        System.out.println("====================================================");
        System.out.println();
        if (queries.size() != processedCount)
        {
            System.out.println("WARNING: unique queries differs from queries processed.");
        }
        System.out.println(String.format("Index type: %s", factory.indexType()));
        System.out.println(String.format("Thread count: %d", threadCount));
        System.out.println(String.format("Unique queries: %d", queries.size()));
        System.out.println(String.format("Queries processed: %d", processedCount));
        System.out.println(String.format("Elapsed time: %f", elapsedTime));
//        System.out.println(String.format("Total parsing latency: %f", parsingLatency));
//        System.out.println(String.format("Total planning latency: %f", plannignLatency));
//        System.out.println(String.format("Total matching latency: %f", matchingLatency));
        System.out.println(String.format("Mean query latency: %f", totalLatency / processedCount));
//        System.out.println(String.format("Planning overhead (\%): %f", overheadLatency / totalLatency));
        System.out.println(String.format("QPS: %f", processedCount / elapsedTime));
    }


    public static void main( String arg[] ) throws Exception {
        SimpleJSAP jsap = new SimpleJSAP( GenerateBitFunnelChunks.class.getName(), "Builds an index (creates batches, combines them, and builds a term map).",
                new Parameter[] {
                        new UnflaggedOption( "indexType", JSAP.STRING_PARSER, JSAP.REQUIRED, "The index type (lucene or mg4j)." ),
                        new UnflaggedOption( "basename", JSAP.STRING_PARSER, JSAP.REQUIRED, "The index basename." ),
                        new UnflaggedOption( "queries", JSAP.STRING_PARSER, JSAP.REQUIRED, "The query log file. One query per line." ),
                        new UnflaggedOption( "outfile", JSAP.STRING_PARSER, JSAP.REQUIRED, "The output file with match counts and timings for each query." ),
                        new UnflaggedOption( "threads", JSAP.INTSIZE_PARSER, JSAP.REQUIRED, "The maximum number of threads to use." ),
                });

        JSAPResult jsapResult = jsap.parse( arg );
        if ( !jsap.messagePrinted() ) {
            QueryProcessorFactory factory = null;
            String indexType = jsapResult.getString("indexType");
            if (indexType.equals("mg4j")) {
                Mg4jIndex index = new Mg4jIndex(jsapResult.getString( "basename" ));
                factory = new QueryProcessorFactory(index);
            }
            else if (indexType.equals("lucene")) {
                LuceneIndex index = new LuceneIndex(jsapResult.getString( "basename" ));
                factory = new QueryProcessorFactory(index);
            }

            if (factory == null)
            {
                System.out.println(String.format("Unknown index type %s. Valid types are lucene and mg4j.", indexType));
            }
            else {
                QueryLogRunner runner = new QueryLogRunner(factory, jsapResult.getString( "queries" ));
                runner.go(jsapResult.getInt( "threads" ), Paths.get(jsapResult.getString( "outfile" )));
            }
        }
    }
}
