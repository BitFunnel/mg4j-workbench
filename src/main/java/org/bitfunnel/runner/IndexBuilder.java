package org.bitfunnel.runner;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;


public class IndexBuilder {
    public static void main(String[] args) throws IOException, InterruptedException {
        if (args.length != 3) {
            System.out.println("Usage: [index directory] [manifest] [thread count");
            return;
        }

        String indexDirectory = args[0];
        String manifestFilename = args[1];
        int threadCount = Integer.parseInt(args[2]);

        // Lucene setup.
        // We use MMapDirectory instead of RAMDirectory because Lucene documentation recommends MMapDirectory for better
        // performance with "large" (> 100MB) indicies.
        // However, other people have observed minimal differences between the two:
        // http://blog.mikemccandless.com/2012/07/lucene-index-in-ram-with-azuls-zing-jvm.html
        // With our setup, ingestion is significantly slower and querying is marginally faster when using MMapDirectory.
        // The speedup in query speed is within the normal variance between runs whereas the slowdown in ingestion speed is
        // large and noticable.
        Directory dir = new MMapDirectory(Paths.get(indexDirectory));
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        ExecutorCompletionService completionService = new ExecutorCompletionService(executor);

        ingestDocuments(manifestFilename, dir, threadCount, completionService);
        System.out.println("Before ExecutorService shutdownNow().");
        executor.shutdownNow();
        System.out.println("ExecutorService shutdownNow() returned.");
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
