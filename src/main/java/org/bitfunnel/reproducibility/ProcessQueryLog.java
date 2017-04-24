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
import it.unimi.di.big.mg4j.query.IntervalSelector;
import it.unimi.di.big.mg4j.query.QueryEngine;
import it.unimi.di.big.mg4j.query.SelectedInterval;
import it.unimi.di.big.mg4j.query.parser.SimpleParser;
import it.unimi.di.big.mg4j.search.DocumentIteratorBuilderVisitor;
import it.unimi.di.big.mg4j.search.score.BM25Scorer;
import it.unimi.di.big.mg4j.search.score.DocumentScoreInfo;
import it.unimi.dsi.fastutil.objects.Object2ReferenceOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.Reference2DoubleOpenHashMap;
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


public class ProcessQueryLog {
    private final Index text;
    private final Index title;
    private final Object2ReferenceOpenHashMap<String,Index> indexMap;
    private final Object2ReferenceOpenHashMap<String, TermProcessor> termProcessors;
    private final QueryEngine engine;

    public ProcessQueryLog(String basename) throws Exception
    {
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

        		/* Optionally, we can score the results. Here we use a state-of-art ranking
		 * function, BM25, which requires document sizes. */
        engine.score( new BM25Scorer() );

		/* Optionally, we can weight the importance of each index. To do so, we have to pass a map,
		 * and again we use the handy fastutil constructor. Note that setting up a BM25F scorer
		 * would give much better results, but we want to keep it simple. */
        engine.setWeights( new Reference2DoubleOpenHashMap<Index>( new Index[] { text, title }, new double[] { 1, 2 } ) );

		/* Optionally, we can use an interval selector to get intervals representing matches. */
        engine.intervalSelector = new IntervalSelector();
    }

    // TODO: Figure out exception handling strategy.
    int ProcessOneQuery(String query) throws Exception {
        // TODO: Reuse the ObjectArrayList for performance.

        System.out.println("Processing query " + query);

		/* We are ready to run our query. We just need a list to store its results. The list is made
		 * of DocumentScoreInfo objects, which comprise a document id, a score, and possibly an
		 * info field that is generic. Here the info field is a map from indices to arrays
		 * of selected intervals. This part will be empty if we do not set an interval selector. */
        ObjectArrayList<DocumentScoreInfo<Reference2ObjectMap<Index, SelectedInterval[]>>> result =
                new ObjectArrayList<DocumentScoreInfo<Reference2ObjectMap<Index,SelectedInterval[]>>>();

		/* The query engine can return any subsegment of the results of a query. Here we grab the first 20 results. */
		// TODO: Increase the length to handle all possibilities.
        // TODO: Investigate whether engine supports count queries.
        // TODO: Figure out how to disable DEBUG spew.
        //       https://www.slf4j.org/faq.html#logging_performance
        //       Look at QueryEngine.java, line 257. It seems the mg4j does not use parameterized messages.
        //       Probably want to use the query array variant on line 298.
        // TODO: Make multithreaded.
        engine.process( query, 0, 20, result );

        for( DocumentScoreInfo<Reference2ObjectMap<Index, SelectedInterval[]>> dsi : result ) {
            System.out.println( "  " + dsi.document + " " + dsi.score );
        }

        System.out.println("ResultCount: " + result.size());

        return result.size();
    }


    // TODO: second argument should be list of queries.
    // TODO: third argument should be output file.
    public static void main( String arg[] ) throws Exception {
        ProcessQueryLog processor = new ProcessQueryLog(arg[0]);
        processor.ProcessOneQuery("dog");
        processor.ProcessOneQuery("cat");

        PrintQueries(Paths.get("d:/git/mg4j-workbench/data/small/queries10.txt"));
    }

    public static void PrintQueries(Path file) {
        List<String> list = null;
        try (Stream<String> lines = Files.lines(file)) {
            list = lines.collect(Collectors.toList());

            for (String query : list) {
                System.out.println(query);
            }
        } catch (java.io.IOException e) {
            System.out.println("Failed to load queries.");
        }
    }
}
