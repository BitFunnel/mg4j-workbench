package org.bitfunnel.reproducibility;


import com.martiansoftware.jsap.*;
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
import it.unimi.dsi.lang.MutableString;
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


public class IndexExporter {
    private final Index text;
    private final Index title;
    private final Object2ReferenceOpenHashMap<String,Index> indexMap;
    private final Object2ReferenceOpenHashMap<String, TermProcessor> termProcessors;

    private final ExperimentalQueryEngine engine;
    ObjectArrayList<DocumentScoreInfo<Reference2ObjectMap<Index, SelectedInterval[]>>> results;


    // TODO: Add javadoc
    public IndexExporter(String basename) throws IllegalAccessException, InvocationTargetException, NoSuchMethodException, IOException, InstantiationException, URISyntaxException, ConfigurationException, ClassNotFoundException {
        // Load and configure the index.
        text = Index.getInstance( basename + "-text?inmemory=1", true, true );
        title = Index.getInstance( basename + "-title?inmemory=1", true, true );

        indexMap = new Object2ReferenceOpenHashMap<String,Index>(
            new String[] { "text", "title" }, new Index[] { text, title } );
        termProcessors = new Object2ReferenceOpenHashMap<String,TermProcessor>(
            new String[] { "text", "title" },
            new TermProcessor[] { text.termProcessor, title.termProcessor } );;

        engine = new ExperimentalQueryEngine(
            new SimpleParser( indexMap.keySet(), "text", termProcessors ),
            new DocumentIteratorBuilderVisitor( indexMap, text, 1000 ),
            indexMap);

        results =
            new ObjectArrayList<DocumentScoreInfo<Reference2ObjectMap<Index,SelectedInterval[]>>>();
    }


    void exportIndex(Path basename) throws QueryParserException, QueryBuilderVisitorException, IOException {
        FileOutputStream docsFile = new FileOutputStream(basename + ".docs");
        LittleEndianIntStream docsStream = new LittleEndianIntStream(docsFile);

        FileOutputStream freqsFile = new FileOutputStream(basename + ".freqs");
        LittleEndianIntStream freqsStream = new LittleEndianIntStream(freqsFile);

        // Ensure we are safe casting the number of documents and posting list lengths to int.
        if (text.numberOfDocuments > Integer.MAX_VALUE)
        {
            // TODO: Use a different exception type here.
            throw new IOException("IndexExporter.go(): Document index out of range.");
        }

        System.out.println(String.format("Converting %d documents.", text.numberOfDocuments));
        System.out.println(String.format("Term count: %d", text.termMap.size()));
        docsStream.putInt(1);
        docsStream.putInt((int)text.numberOfDocuments);

        for (int i = 0 ; i < text.termMap.size(); ++i) {
            if (i % 10000 == 0)
            {
                System.out.println(String.format("  term %d", i));
            }

            String term = text.termMap.list().get(i).toString();

            engine.process(term, 0, 1000000000, results);

            docsStream.putInt(results.size());
            freqsStream.putInt(results.size());

            int counter = 0;
            for (DocumentScoreInfo<Reference2ObjectMap<Index, SelectedInterval[]>> r : results) {
                docsStream.putInt((int)r.document);

                // DESIGN NOTE: For now, generate a placeholder frequencies file that represents
                // every terms as having a frequency of 1. This should suffice for Partitioned Elias-Fano
                // tests that don't access the frequencies, but require the existence of the file.
                freqsStream.putInt(1);
                ++counter;
            }
        }
        docsStream.close();
        docsFile.close();

        freqsStream.close();
        freqsFile.close();
    }


    void convertQueryLog(Path queryLog, Path outputBaseName) throws IOException {
        List<String> queries = ReadQueries(queryLog);

        //
        // Filter out queries that contain terms not in index, then convert
        // terms in remaining queries to corresponding ints.
        //

        List<String> filteredQueries = new ArrayList<String>(queries.size());
        List<String> filteredIntQueries = new ArrayList<String>(queries.size());

        for (String query : queries) {
            String[] terms = query.split(" ");
            MutableString converted = new MutableString();
            Boolean allTermsInIndex = true;
            for (String term : terms) {
                Object termId = text.termMap.get(term);
                if (termId == null){
                    allTermsInIndex = false;
                    break;
                }

                if (converted.length() > 0) {
                    converted.append(' ');
                }
                converted.append(termId);
            }

            if (allTermsInIndex) {
                filteredQueries.add(query);
                filteredIntQueries.add(converted.toString());
            }
        }

        //
        // Write filtered and converted query logs to files.
        //

        String filteredFile = outputBaseName + "-filtered.txt";
        System.out.println(String.format("Writing filtered queries to \"%s\"", filteredFile));
        WriteQueries(filteredQueries, Paths.get(filteredFile));

        String filteredIntFile = outputBaseName + "-filtered-ints.txt";
        System.out.println(String.format("Writing filtered int queries to \"%s\"", filteredIntFile));
        WriteQueries(filteredIntQueries, Paths.get(filteredIntFile));

        System.out.println(String.format("Input query count: %d", queries.size()));
        System.out.println(String.format("Filtered query count: %d", filteredQueries.size()));
        System.out.println(String.format("Retained %2.1f%% of queries.", (double)filteredQueries.size() / (double)queries.size() * 100.0));
    }


    public static List<String> ReadQueries(Path path) throws IOException {
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


    public static void WriteQueries(List<String> queries, Path path) throws IOException {
        File outFile = path.toFile();
        BufferedWriter writer =  null;
        try {
            writer = new BufferedWriter(new FileWriterWithEncoding(outFile, StandardCharsets.UTF_8));

            for (int i = 0; i < queries.size(); ++i) {
                writer.write(queries.get(i));
                writer.newLine();
            }
        }
        finally {
            if (writer != null) {
                writer.close();
            }
        }
    }


    public static void main( String arg[] ) throws Exception {
        SimpleJSAP jsap = new SimpleJSAP( IndexExporter.class.getName(),
                "Exports an mg4j index in a format suitable for creating a Partitioned Elias-Fano index.",
                new Parameter[] {
                        new UnflaggedOption( "inbasename", JSAP.STRING_PARSER, JSAP.REQUIRED, "The mg4j index basename." ),
                        new UnflaggedOption( "outbasename", JSAP.STRING_PARSER, JSAP.REQUIRED, "The output basename." ),
                        new Switch( "index", JSAP.NO_SHORTFLAG, "index", "Export the index for Partitioned Elias-Fano." ),
                        new FlaggedOption( "queries", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, JSAP.NO_SHORTFLAG, "queries", "Query log input file." ),
                });

        JSAPResult jsapResult = jsap.parse( arg );
        if ( !jsap.messagePrinted() ) {
            if (jsapResult.getBoolean("index") || jsapResult.userSpecified("queries")) {
                IndexExporter exporter = new IndexExporter(jsapResult.getString( "inbasename" ));

                // Export index if requested.
                if (jsapResult.getBoolean("index")) {
                    System.out.println();
                    System.out.println(String.format("Exporting index file \"%s\" to basename \"%s\".",
                            jsapResult.getString("inbasename"),
                            jsapResult.getString("outbasename")));
                    exporter.exportIndex(Paths.get(jsapResult.getString( "outbasename" )));
                }

                if (jsapResult.userSpecified("queries")) {
                    System.out.println();
                    System.out.println(String.format("Converting query file \"%s\" to basename \"%s\".",
                            jsapResult.getString("queries"),
                            jsapResult.getString( "outbasename" )));
                    exporter.convertQueryLog(Paths.get(jsapResult.getString( "queries" )),
                                             Paths.get(jsapResult.getString( "outbasename" )));
                }
            }
            else {
                System.out.println("Must specify at least one option.");
                System.out.println();
                // TODO: print out executable name.
                System.out.println(jsap.getUsage());
                System.out.println(jsap.getHelp());
            }
        }
    }
}
