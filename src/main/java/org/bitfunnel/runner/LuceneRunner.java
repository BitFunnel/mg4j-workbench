package org.bitfunnel.runner;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

class LuceneRunner {
public static void main(String[] args) throws IOException, InterruptedException {
    // Open manifest.
    // String manifestFilename = "/home/danluu/dev/wikipedia.100.150/Manifest.Short.txt";
    String manifestFilename = "/home/danluu/dev/wikipedia.100.150/Manifest.txt";

    // Lucene setup.
    Directory dir = new RAMDirectory();
    int numThreads = 8;
    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    ExecutorCompletionService completionService = new ExecutorCompletionService(executor);

    ingestDocuments(manifestFilename, dir, numThreads, completionService);

    // Now search the index:
    DirectoryReader ireader = null;
    ireader = DirectoryReader.open(dir);

    IndexSearcher isearcher = new IndexSearcher(ireader);

    String queryFilename = "/home/danluu/dev/wikipedia.100.200.old/terms.d20.txt";
    String[] queryLog = getLinesFromFile(queryFilename);


    AtomicInteger numCompleted = new AtomicInteger();
    AtomicInteger numHits = new AtomicInteger();
    System.out.println("Query warmup.");
    executeQueries(numThreads, completionService, isearcher, queryLog, numCompleted, numHits);
    numCompleted.set(0);
    numHits.set(0);
    System.out.println("Querying.");
    System.gc();
    long queryStartTime = System.currentTimeMillis();
    executeQueries(numThreads, completionService, isearcher, queryLog, numCompleted, numHits);
    long queryDoneTime = System.currentTimeMillis();

    executor.shutdown();
    executor.awaitTermination(300, TimeUnit.SECONDS);

    long queryDuration = queryDoneTime - queryStartTime;
    Double qps = Double.valueOf(queryLog.length / (Double.valueOf(queryDuration)) * 1000.0);
    System.out.println("queryDuration: " + queryDuration);
    System.out.println("queryLog.size(): " + queryLog.length);
    System.out.println("queries run: " + numCompleted.get());
    System.out.println("qps: " + qps);
    System.out.println("total matches: " + numHits.get());

}

    private static void executeQueries(int numThreads, ExecutorCompletionService completionService, IndexSearcher isearcher, String[] queryLog, AtomicInteger numCompleted, AtomicInteger numHits) throws InterruptedException {
        IntStream.range(0, numThreads).forEach(
                t -> {
                    Callable task = () -> {
                        // Add some kind of collector so we don't optimize away all work.
                        TotalHitCountCollector collector = new TotalHitCountCollector();
                        while (true) {
                            int idx = numCompleted.getAndIncrement();
                            if (idx >= queryLog.length) {
                                numCompleted.decrementAndGet();
                                numHits.addAndGet(collector.getTotalHits());
                                return null;
                            }
                            executeQuery(idx, queryLog, isearcher, collector);
                        }
                    };
                    completionService.submit(task);
                }
        );

        for (int i = 0; i < numThreads; ++i) {
            completionService.take();
        }
    }

    private static void ingestDocuments(String manifestFilename, Directory dir, int numThreads, ExecutorCompletionService completionService) throws IOException, InterruptedException {
        String[] chunkfileNames = getLinesFromFile(manifestFilename);

        IndexWriterConfig config = new IndexWriterConfig(new StandardAnalyzer());
        IndexWriter writer =  new IndexWriter(dir, config);

        AtomicInteger fileIdx = new AtomicInteger();
        System.out.println("Ingesting " + chunkfileNames.length + " chunkfiles.");
        long ingestStartTime = System.currentTimeMillis();
        IntStream.range(0, numThreads).forEach(
                t -> {
                    Callable task = () -> {
                        try {
                            DocumentProcessor processor = new DocumentProcessor(writer);
                            while (true) {
                                int idx = fileIdx.getAndIncrement();
                                if (idx >= chunkfileNames.length) {
                                    fileIdx.decrementAndGet();
                                    return null;
                                }
                                InputStream inputStream = new FileInputStream(chunkfileNames[idx]);
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

        for (int i = 0; i < numThreads; ++i) {
            completionService.take();
        }

        // Commit index.
        writer.commit();
        long ingestDoneTime = System.currentTimeMillis();

        System.out.println("numIngested: " + fileIdx.get());
    }

    private static void executeQuery(int index, String[] queries, IndexSearcher isearcher, TotalHitCountCollector collector) throws IOException {
        String[] terms= queries[index].split(" ");
        BooleanQuery.Builder queryBuilder = new BooleanQuery.Builder();
        for (String termText : terms) {
            // field seems to be "00" for title, "01" for body.
            Term term = new Term("01", termText);
            TermQuery termQuery = new TermQuery(term);
            queryBuilder.add(termQuery, BooleanClause.Occur.MUST);
        }
        BooleanQuery tempQuery = queryBuilder.build();
        ConstantScoreQuery query = new ConstantScoreQuery(tempQuery);
        isearcher.search(query, collector);
    }

    private static String[] getLinesFromFile(String manifestFilename) throws IOException {
        Path filepath = new File(manifestFilename).toPath();
        List<String> stringList;
        stringList= Files.readAllLines(filepath);
        String[] stringArray = stringList.toArray(new String[]{});
        return stringArray;
    }
}