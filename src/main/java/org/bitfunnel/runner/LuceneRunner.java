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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

class LuceneRunner {
public static void main(String[] args) throws IOException, InterruptedException {
    // Open manifest.
    String manifestFilename = "/home/danluu/dev/wikipedia.100.150/Manifest.Short.txt";
    // String manifestFilename = "/home/danluu/dev/wikipedia.100.150/Manifest.txt";
    List<String> stringList = getLinesFromFile(manifestFilename);

    // Lucene setup.
    Directory dir = new RAMDirectory();
    IndexWriterConfig config = new IndexWriterConfig(new StandardAnalyzer());
    IndexWriter writer;
    writer = new IndexWriter(dir, config);

    DocumentProcessor processor = new DocumentProcessor(writer);

    long ingestStartTime = System.currentTimeMillis();
    // Ingest chunkfiles into Index.
    for (String chunkfile : stringList) {
        System.out.println(chunkfile);
        InputStream inputStream;
        inputStream = new FileInputStream(chunkfile);
        CorpusFile corpus = new CorpusFile(inputStream);
        corpus.process(processor);
    }

    // Commit index.
    writer.commit();
    long ingestDoneTime = System.currentTimeMillis();

    // Now search the index:
    DirectoryReader ireader = null;
    ireader = DirectoryReader.open(dir);

    IndexSearcher isearcher = new IndexSearcher(ireader);

//    // Debug prints.
//    try {
//        Fields fields = MultiFields.getFields(ireader);
//        for (String field : fields) {
//            System.out.println("Field: " + field);
//            Terms terms = null;
//                terms = fields.terms(field);
//                System.out.println("terms.size(): " + terms.size());
//                TermsEnum termsEnum = terms.iterator();
//                while (termsEnum.next() != null) {
//                    System.out.println(termsEnum.term().utf8ToString());
//                }
//        }
//    } catch (IOException e) {
//        e.printStackTrace();
//        return;
//    }



    String queryFilename = "/home/danluu/dev/wikipedia.100.200.old/terms.d20.txt";
    List<String> tempQueryLog = getLinesFromFile(queryFilename);
    String[] queryLog = tempQueryLog.toArray(new String[]{});

    AtomicInteger numCompleted = new AtomicInteger(0);
    AtomicInteger numHits = new AtomicInteger(0);
    int numThreads = 8;
    ExecutorService executor = Executors.newFixedThreadPool(numThreads);

    long queryStartTime = System.currentTimeMillis();
    IntStream.range(0, numThreads).forEach(
            t -> {
                Runnable task = () -> {
                    try {
                        // Add something so we don't optimize away all work.
                        TotalHitCountCollector collector = new TotalHitCountCollector();
                        while (true) {
                            int idx = numCompleted.getAndIncrement();
                            if (idx >= queryLog.length) {
                                numCompleted.decrementAndGet();
                                numHits.addAndGet(collector.getTotalHits());
                                return;
                            }
                            executeQuery(idx, queryLog, isearcher, collector);
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                        return;
                    }
                };
                executor.execute(task);
            }
    );
    executor.shutdown();
    executor.awaitTermination(300, TimeUnit.SECONDS);
//    for (int i = 0; i < queryLog.length; ++i) {
//        executeQuery(i, queryLog, isearcher, collector);
//    }
    long queryDoneTime = System.currentTimeMillis();
    long queryDuration = queryDoneTime - queryStartTime;
    Double qps = Double.valueOf(queryLog.length / (Double.valueOf(queryDuration)) * 1000.0);
    System.out.println("queryDuration: " + queryDuration);
    System.out.println("queryLog.size(): " + queryLog.length);
    System.out.println("queries run: " + numCompleted.get());
    System.out.println("qps: " + qps);
    System.out.println("total matches: " + numHits.get());

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

    private static List<String> getLinesFromFile(String manifestFilename) throws IOException {
        Path filepath = new File(manifestFilename).toPath();
        List<String> stringList;
        stringList= Files.readAllLines(filepath);
        return stringList;
    }
}