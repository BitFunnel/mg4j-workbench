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

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;
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
        FileOutputStream docsFile = new FileOutputStream(basename + "-docs.bin");
        LittleEndianIntStream docsStream = new LittleEndianIntStream(docsFile);

        FileOutputStream freqsFile = new FileOutputStream(basename + "-freqs.bin");
        LittleEndianIntStream freqsStream = new LittleEndianIntStream(freqsFile);

        // Ensure we are safe casting the number of documents and posting list lengths to int.
        if (text.numberOfDocuments > Integer.MAX_VALUE)
        {
            // TODO: Use a different exception type here.
            throw new IOException("IndexExporter.go(): Document index out of range.");
        }

        System.out.println(String.format("Document count: {%x}", text.numberOfDocuments));
        docsStream.putInt(1);
        docsStream.putInt((int)text.numberOfDocuments);

        for (int i = 0 ; i < 10; ++i) {
            String term = text.termMap.list().get(i).toString();

            engine.process(term, 0, 1000000000, results);

            System.out.println(String.format("%s (%x)", term, results.size()));

            docsStream.putInt(results.size());
            freqsStream.putInt(results.size());

            //DocumentScoreInfo<Reference2ObjectMap<Index, SelectedInterval[]>> r = results.get(0);
            int counter = 0;
            for (DocumentScoreInfo<Reference2ObjectMap<Index, SelectedInterval[]>> r : results) {
                System.out.println(String.format("  %x: %x", counter, r.document));
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


    void convertQueryLog(Path queryLog) throws IOException {
        List<String> queries = LoadQueries(queryLog);

        // TODO: Filter log to exclude queries with null terms.
        // TODO: Two output streams, one for converted queries and one for the filtered log.

        for (String query : queries) {
            String[] terms = query.split(" ");
            MutableString converted = new MutableString();
            for (String term : terms) {
                System.out.println(String.format("  \"%s\"", term));
                if (converted.length() > 0) {
                    converted.append(' ');
                }
                converted.append(text.termMap.get(term));
            }
            System.out.println(String.format("\"%s\" ==> \"%s\"", query, converted));
        }
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
                    System.out.println(String.format("Export index %s to %s.",
                            jsapResult.getString("inbasename"),
                            jsapResult.getString("outbasename")));
                    // TODO: Restore next line
//                    exporter.exportIndex(Paths.get(jsapResult.getString( "outbasename" )));
                }

                if (jsapResult.userSpecified("queries")) {
                    System.out.println(String.format("Convert query file %s.", jsapResult.getString("queries")));
                    exporter.convertQueryLog(Paths.get(jsapResult.getString( "queries" )));
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
