package org.bitfunnel.runner;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.RAMDirectory;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

class LuceneRunner {

public static void main(String[] args) throws IOException, InterruptedException {
    if (args.length != 3) {
        System.out.println("Usage: [manifest] [query log] [num threads]");
        return;
    }

    String manifestFilename = args[0];

    String queryFilename = args[1];
    String[] queryLog = getLinesFromFile(queryFilename);

    // Lucene setup.
    // We use MMapDirectory instead of RAMDirectory because Lucene documentation recommends MMapDirectory for better
    // performance with "large" (> 100MB) indicies.
    // However, other people have observed minimal differences between the two:
    // http://blog.mikemccandless.com/2012/07/lucene-index-in-ram-with-azuls-zing-jvm.html
    // With our setup, ingestion is significantly slower and querying is marginally faster when using MMapDirectory.
    // The speedup in query speed is within the normal variance between runs whereas the slowdown in ingestion speed is
    // large and noticable.
    Directory dir = new MMapDirectory(Paths.get("/tmp/lucene-measure"));
    int threadCount = Integer.parseInt(args[2]);
    ExecutorService executor = Executors.newFixedThreadPool(threadCount);
    ExecutorCompletionService completionService = new ExecutorCompletionService(executor);

    ingestDocuments(manifestFilename, dir, threadCount, completionService);

    // Now search the index:
    DirectoryReader ireader = null;
    ireader = DirectoryReader.open(dir);
    System.out.println(String.format("Document count: %d", ireader.numDocs()));

    IndexSearcher isearcher = new IndexSearcher(ireader);

    // Measure execution latencies. Note that this methodology is not sufficient for measuring "real" tail latency.
    // Also note that throughput measurements for the paper were done with individual query measurement turned off, although the overhead should be low.
    // long[] queryTimes = new long[queryLog.length];


    AtomicInteger numCompleted = new AtomicInteger();
    AtomicInteger numHits = new AtomicInteger();
    System.out.println(String.format("Query warmup: processing %d queries with %d threads.", queryLog.length, threadCount));
    // executeQueries(threadCount, completionService, isearcher, queryLog, numCompleted, numHits, queryTimes);
    executeQueries(threadCount, completionService, isearcher, queryLog, numCompleted, numHits);
    numCompleted.set(0);
    numHits.set(0);
    System.out.println(String.format("Query measurement: processing %d queries with %d threads.", queryLog.length, threadCount));
    System.gc();
    long queryStartTime = System.currentTimeMillis();
    // executeQueries(threadCount, completionService, isearcher, queryLog, numCompleted, numHits, queryTimes);
    executeQueries(threadCount, completionService, isearcher, queryLog, numCompleted, numHits);
    long queryDoneTime = System.currentTimeMillis();

    executor.shutdown();
    executor.awaitTermination(300, TimeUnit.SECONDS);

    long queryDuration = queryDoneTime - queryStartTime;
    double elapsedTime = queryDuration / 1000.0;
    Double qps = queryLog.length / elapsedTime;

    System.out.println();
    System.out.println("====================================================");
    System.out.println();
    System.out.println(String.format("Thread count: %d", threadCount));
    System.out.println(String.format("Query count: %d", queryLog.length));
    System.out.println(String.format("Queries run: %d", numCompleted.get()));
    System.out.println(String.format("Total time: %f", elapsedTime));
    System.out.println(String.format("QPS: %f", qps));


    // System.out.println("total matches: " + numHits.get());

//    java.util.Arrays.sort(queryTimes);
//    int maxIdx = queryTimes.length - 1;
//    int[] timeIdx = {
//            maxIdx - maxIdx / 2,
//            maxIdx - maxIdx / 10,
//            maxIdx - maxIdx / 100,
//            maxIdx - maxIdx / 1000,
//            maxIdx};
//
//    for (int idx : timeIdx) {
//        System.out.print(((double)queryTimes[idx]) / 1e9 + ",");
//    }

}

    //private static void executeQueries(int threadCount, ExecutorCompletionService completionService, IndexSearcher isearcher, String[] queryLog, AtomicInteger numCompleted, AtomicInteger numHits, long[] queryTimes) throws InterruptedException {
    private static void executeQueries(int threadCount,
                                       ExecutorCompletionService completionService,
                                       IndexSearcher isearcher,
                                       String[] queryLog,
                                       AtomicInteger numCompleted,
                                       AtomicInteger numHits) throws InterruptedException {
        IntStream.range(0, threadCount).forEach(
                t -> {
                    Callable task = () -> {
                        // This collector is bogus. We add a colletor that returns Lucene IDs, which can then be looked up
                        // to get wikipedia IDs. However, since we have no use for the IDs it's possible the JVM optimizes
                        // all of this out. Or maybe it doesn't. If it were up to me, I wouldn't write this code this way.
                        // Sorry!
                        MatchingCollector collector = new MatchingCollector();
                        while (true) {
                            int idx = numCompleted.getAndIncrement();
                            if (idx >= queryLog.length) {
                                numCompleted.decrementAndGet();
                                // numHits.addAndGet(collector.getTotalHits());
                                collector.getDocIds();
                                return null;
                            }
//                            long singleStartTime = System.nanoTime();
                              executeQuery(idx, queryLog, isearcher, collector);
//                            long singleEndTime = System.nanoTime();
//                            queryTimes[idx] = singleEndTime - singleStartTime;
                        }
                    };
                    completionService.submit(task);
                }
        );

        for (int i = 0; i < threadCount; ++i) {
            completionService.take();
        }
    }

    private static void ingestDocuments(String manifestFilename,
                                        Directory dir,
                                        int threadCount,
                                        ExecutorCompletionService completionService) throws IOException, InterruptedException {
        String[] chunkfileNames = getLinesFromFile(manifestFilename);

        IndexWriterConfig config = new IndexWriterConfig(new StandardAnalyzer());
        IndexWriter writer =  new IndexWriter(dir, config);

        AtomicInteger fileIndex = new AtomicInteger();
        System.out.println(String.format("Ingesting %d chunk files with %d threads.",
                                         chunkfileNames.length,
                                         threadCount));
        long ingestStartTime = System.currentTimeMillis();
        IntStream.range(0, threadCount).forEach(
                t -> {
                    Callable task = () -> {
                        try {
                            DocumentProcessor processor = new DocumentProcessor(writer);
                            while (true) {
                                int index = fileIndex.getAndIncrement();
                                if (index >= chunkfileNames.length) {
                                    fileIndex.decrementAndGet();
                                    return null;
                                }
                                System.out.println(String.format("  %s", chunkfileNames[index]));
                                InputStream inputStream = new FileInputStream(chunkfileNames[index]);
                                CorpusFile corpus = new CorpusFile(inputStream);
                                corpus.process(processor);
                            }
                        } catch (IOException e) {
                            e.printStackTrace();
                            return null;
                        }
                    };
                    completionService.submit(task);
                }
        );

        for (int i = 0; i < threadCount; ++i) {
            completionService.take();
        }

        // Commit index.
        writer.commit();
        long ingestDoneTime = System.currentTimeMillis();

        System.out.println(String.format("Ingested %d chunk files in %f seconds.",
                fileIndex.get(),
                (ingestDoneTime - ingestStartTime) / 1000.0));
    }

    private static void executeQuery(int index, String[] queries, IndexSearcher isearcher, Collector collector) throws IOException {
        String[] terms= queries[index].split(" ");
        // Note: we also tried using Lucene's "Classic" query parser. It gets the same results but it's slightly slower,
        // presumebly due to the extra overhead of having to actually parse the string.
        BooleanQuery.Builder queryBuilder = new BooleanQuery.Builder();
        for (String termText : terms) {
            // field seems to be "00" for title, "01" for body.
            Term term = new Term("01", termText);
            TermQuery termQuery = new TermQuery(term);
            // Using FILTER instead of MUST because FILTER doesn't score.
            queryBuilder.add(termQuery, Occur.FILTER);
        }
        BooleanQuery tempQuery = queryBuilder.build();
        ConstantScoreQuery query = new ConstantScoreQuery(tempQuery);
        isearcher.search(query, collector);
    }

    private static String[] getLinesFromFile(String manifestFilename) throws IOException {
        Path filepath = new File(manifestFilename).toPath();
        List<String> stringList;
        // WARNING: We use ISO_8859_1 because the TREC efficiency topics query log seems to ASCII or something similar.
        // In general, the chunk files we read can be arbitrary UTF-8. This may cause some queries to return fewer terms
        // than they should. Because this is being used as a baseline and this problem should only cause Lucene
        // performance to be higher than it should be, this seems acceptable.
        stringList= Files.readAllLines(filepath, StandardCharsets.ISO_8859_1);
        String[] stringArray = stringList.toArray(new String[]{});
        return stringArray;
    }
}