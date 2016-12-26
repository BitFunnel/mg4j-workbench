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

class LuceneRunner {
public static void main(String[] args) throws IOException {
    // Open manifest.
    // String manifestFilename = "/home/danluu/dev/wikipedia.100.150/Manifest.Short.txt";
    String manifestFilename = "/home/danluu/dev/wikipedia.100.150/Manifest.txt";
    List<String> stringList = getLinesFromFile(manifestFilename);

    // Lucene setup.
    Directory dir = new RAMDirectory();
    IndexWriterConfig config = new IndexWriterConfig(new StandardAnalyzer());
    IndexWriter writer;
    try {
        writer = new IndexWriter(dir, config);
    } catch (IOException e) {
        e.printStackTrace();
        return;
    }

    DocumentProcessor processor = new DocumentProcessor(writer);
    long ingestStartTime = System.currentTimeMillis();
    // Ingest chunkfiles into Index.
    for (String chunkfile : stringList) {
        System.out.println(chunkfile);
        InputStream inputStream;
        try {
            inputStream = new FileInputStream(chunkfile);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            return;
        }
        CorpusFile corpus = new CorpusFile(inputStream);
        corpus.process(processor);
    }

    // Commit index.
    try {
        writer.commit();
    } catch (IOException e) {
        e.printStackTrace();
        return;
    }
    long ingestDoneTime = System.currentTimeMillis();

    // Now search the index:
    DirectoryReader ireader = null;
    try {
        ireader = DirectoryReader.open(dir);
    } catch (IOException e) {
        e.printStackTrace();
        return;
    }
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


    // Add something so we don't optimize away all work.
    TotalHitCountCollector collector = new TotalHitCountCollector();

    long queryStartTime = System.currentTimeMillis();
    String queryFilename = "/home/danluu/dev/wikipedia.100.200.old/terms.d20.txt";
    List<String> queryLog = getLinesFromFile(queryFilename);
    for (String queryString : queryLog) {
        String[] terms= queryString.split(" ");
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
    long queryDoneTime = System.currentTimeMillis();
    long queryDuration = queryDoneTime - queryStartTime;
    Double qps = Double.valueOf(queryLog.size() / (Double.valueOf(queryDuration)) * 1000.0);
    System.out.println("queryDuration: " + queryDuration);
    System.out.println("queryLog.size(): " + queryLog.size());
    System.out.println("qps: " + qps);

}

    private static List<String> getLinesFromFile(String manifestFilename) throws IOException {
        Path filepath = new File(manifestFilename).toPath();
        List<String> stringList;
        stringList= Files.readAllLines(filepath);
        return stringList;
    }
}