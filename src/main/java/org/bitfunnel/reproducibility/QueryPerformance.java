/*
* MG4J: Managing Gigabytes for Java (big)
*
* Copyright (C) 2009-2016 Sebastiano Vigna
*
*  This library is free software; you can redistribute it and/or modify it
*  under the terms of the GNU Lesser General Public License as published by the Free
*  Software Foundation; either version 3 of the License, or (at your option)
*  any later version.
*
*  This library is distributed in the hope that it will be useful, but
*  WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
*  or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
*  for more details.
*
*  You should have received a copy of the GNU Lesser General Public License
*  along with this program; if not, see <http://www.gnu.org/licenses/>.
*
*/

package org.bitfunnel.reproducibility;

import it.unimi.di.big.mg4j.document.HtmlDocumentFactory;
import it.unimi.di.big.mg4j.index.Index;
import it.unimi.di.big.mg4j.index.TermProcessor;
import it.unimi.di.big.mg4j.query.QueryEngine;
import it.unimi.di.big.mg4j.query.SelectedInterval;
import it.unimi.di.big.mg4j.query.parser.SimpleParser;
import it.unimi.di.big.mg4j.search.DocumentIteratorBuilderVisitor;
import it.unimi.di.big.mg4j.search.score.DocumentScoreInfo;
import it.unimi.dsi.fastutil.objects.Object2ReferenceOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.Reference2ObjectMap;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;


/** A very simple example that shows how to load a couple of indices and run them using
 * a {@linkplain QueryEngine query engine}. First argument is the basename of an index (possibly produced
 * by an {@link HtmlDocumentFactory}) that has fields <code>title</code> and <code>text</code>.
 * Second argument is a query.
 *
 * @author Sebastiano Vigna
 * @since 2.2
 */


public class QueryPerformance {
    private final Index text;
    private final Index title;
    private final Object2ReferenceOpenHashMap<String,Index> indexMap;
    private final Object2ReferenceOpenHashMap<String, TermProcessor> termProcessors;
    private final QueryEngine engine;

    private List<String> queries;

    // Used to record number of matches for each query in queries.
    private int[] matchCounts;

    // Used to record the processing time for each query in queries.
    private long[] timesInNS;


    public QueryPerformance(String basename, String queryLogFile) throws Exception
    {
        //
        // Set up the index and the engine.
        //

        // TODO: use Paths.get(), etc. here. At least deal with trailing '/' and '.'.
        /** First we open our indices. The booleans tell that we want random access to
         * the inverted lists, and we are going to use document sizes (for scoring--see below). */
        text = Index.getInstance( basename + "-text", true, true );
        title = Index.getInstance( basename + "-title", true, true );

		/* We need a map mapping index names to actual indices. Its keyset will be used by the
		 * parser to distinguish correct index names (e.g., "text:foo title:bar"), and the mapping
		 * itself will be used when transforming a query into a document iterator. We use a handy
		 * fastutil array-based constructor. */
        indexMap =
                new Object2ReferenceOpenHashMap<String,Index>(
                    new String[] { "text", "title" }, new Index[] { text, title } );

		/* We now need to map index names to term processors. This is necessary as any processing
		 * applied during indexing must be applied at query time, too. */
        termProcessors =
            new Object2ReferenceOpenHashMap<String,TermProcessor>(
                    new String[] { "text", "title" },
                    new TermProcessor[] { text.termProcessor, title.termProcessor } );

		/* To run a query in a simple way we need a query engine. The engine requires a parser
		 * (which in turn requires the set of index names and a default index), a document iterator
		 * builder, which needs the index map, a default index, and a limit on prefix query
		 * expansion, and finally the index map. */
        engine = new QueryEngine(
                new SimpleParser( indexMap.keySet(), "text", termProcessors ),
                new DocumentIteratorBuilderVisitor( indexMap, text, 1000 ),
                indexMap);


        //
        // Set up the query log
        //

        queries = ReadLines(Paths.get(queryLogFile));

        // DESIGN NOTE: Using arrays of primitives here, instead of an ArrayList of Result objects
        // to avoid allocation for each result.
        matchCounts = new int[queries.size()];
        timesInNS = new long[queries.size()];
    }

    // TODO: Figure out exception handling strategy.
    int ProcessOneQuery(String query) throws Exception {
        // TODO: Reuse the ObjectArrayList for performance.

        // TODO: Investigate whether engine supports returning the match count instead of the actual matches.
        // TODO: Consider using a modified QueryEngine that only returns counts.

		/* We are ready to run our query. We just need a list to store its results. The list is made
		 * of DocumentScoreInfo objects, which comprise a document id, a score, and possibly an
		 * info field that is generic. Here the info field is a map from indices to arrays
		 * of selected intervals. This part will be empty if we do not set an interval selector. */
        ObjectArrayList<DocumentScoreInfo<Reference2ObjectMap<Index, SelectedInterval[]>>> result =
                new ObjectArrayList<DocumentScoreInfo<Reference2ObjectMap<Index,SelectedInterval[]>>>();

		/* The query engine can return any subsegment of the results of a query. Here we grab the first 20 results. */
		// TODO: Increase the length to handle all possibilities.
        // TODO: Figure out how to disable DEBUG spew. Consider using a modified QueryEngine with no debug spew.
        //       https://www.slf4j.org/faq.html#logging_performance
        //       Look at QueryEngine.java, line 257. It seems the mg4j does not use parameterized messages.
        //       Probably want to use the query array variant on line 298.
        engine.process( query, 0, 2000, result );

        return result.size();
    }


    // TODO: Make multithreaded.
    // TODO: Measure total running time, in addition to per-query time.
    void ProcessAllQueries() throws Exception
    {
        // Run multiple times to warm system up. Retain measurements from last run.
        for (int runs = 0; runs < 2; ++runs) {
            for (int i = 0 ; i < queries.size(); ++i) {
                //Instant start = Instant.now();
                long start = System.nanoTime();
                matchCounts[i] = ProcessOneQuery(queries.get(i));
                //Instant end = Instant.now();
                timesInNS[i] = System.nanoTime() - start;
                //timesInNS[i] = Duration.between(start, end).toNanos();
            }
        }

        // TODO: Write measurements to file.
        for (int i = 0 ; i < queries.size(); ++i) {
            System.out.println(String.format("%s,%d,%f", queries.get(i), matchCounts[i], timesInNS[i] * 1e-9));
        }
    }

    // First argument: base name of index.
    // Second argument: path to a query log file.
    // [NOT IMPLEMENTED YET]: Third argument: path to output file where results will be written.
    // TODO: third argument should be output file.
    public static void main( String arg[] ) throws Exception {
        QueryPerformance processor = new QueryPerformance(arg[0], arg[1]);
        processor.ProcessAllQueries();
    }


    public static List<String> ReadLines(Path file) throws IOException {
        List<String> list = null;
        Stream<String> lines = Files.lines(file);
        return lines.collect(Collectors.toList());
    }
}
