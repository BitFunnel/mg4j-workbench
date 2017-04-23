/*
 * MG4J: Managing Gigabytes for Java (big)
 *
 * Copyright (C) 2005-2016 Sebastiano Vigna
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

import it.unimi.di.big.mg4j.document.*;
import it.unimi.di.big.mg4j.tool.*;
import it.unimi.di.big.mg4j.util.MG4JClassParser;
import it.unimi.dsi.io.WordReader;
import it.unimi.dsi.lang.MutableString;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Reader;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.commons.configuration.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;
import com.martiansoftware.jsap.Switch;
import com.martiansoftware.jsap.UnflaggedOption;

/** A BitFunnel chunk file builder.
 *
 * <p>An instance of this class exposes a {@link #run()} method
 * that will convert the {@link DocumentSequence} provided at construction time
 * to a BitFunnel chunk.
 *
 * <p>Additionally, a main method provides easy access to index construction.
 *
 */

public class GenerateBitFunnelChunks {
    final static Logger LOGGER = LoggerFactory.getLogger( GenerateBitFunnelChunks.class );


    // TODO: Remove throws java.lang.Exception. This is too general.
    @SuppressWarnings({ "unchecked", "resource" })
    public static void main( final String[] arg ) throws  Exception, JSAPException, InvocationTargetException, NoSuchMethodException, IllegalAccessException, ConfigurationException, ClassNotFoundException, IOException, InstantiationException, URISyntaxException {

        SimpleJSAP jsap = new SimpleJSAP( GenerateBitFunnelChunks.class.getName(), "Builds an index (creates batches, combines them, and builds a term map).",
                new Parameter[] {
                        new FlaggedOption( "sequence", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'S', "sequence", "A serialised document sequence that will be used instead of stdin." ),
                        new FlaggedOption( "delimiter", JSAP.INTEGER_PARSER, Integer.toString( Scan.DEFAULT_DELIMITER ), JSAP.NOT_REQUIRED, 'd', "delimiter", "The document delimiter (when indexing stdin)." ),
                        new FlaggedOption( "factory", MG4JClassParser.getParser(), IdentityDocumentFactory.class.getName(), JSAP.NOT_REQUIRED, 'f', "factory", "A document factory with a standard constructor (when indexing stdin)." ),
                        new FlaggedOption( "property", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'p', "property", "A 'key=value' specification, or the name of a property file (when indexing stdin)." ).setAllowMultipleDeclarations( true ),
                        // TODO: Decide whether to implement "downcase" switch. May want to hard-code this behavior.
                        new Switch( "downcase", JSAP.NO_SHORTFLAG, "downcase", "A shortcut for setting the term processor to the downcasing processor." ),
                        new UnflaggedOption( "chunkFile", JSAP.STRING_PARSER, JSAP.REQUIRED, "The name of the BitFunnel chunk file." )
        });

        JSAPResult jsapResult = jsap.parse( arg );
        if ( jsap.messagePrinted() ) return;

        final DocumentSequence documentSequence = Scan.getSequence( jsapResult.getString( "sequence" ), jsapResult.getClass( "factory" ), jsapResult.getStringArray( "property" ), jsapResult.getInt( "delimiter" ), LOGGER );

        // TODO: put following code in run() method.

        Path chunkFile = Paths.get(jsapResult.getString( "chunkFile" ));
        if (Files.exists(chunkFile)) {
            System.out.println("Error: chunk file " + chunkFile.getFileName() + " already exists.");
        }
        else {
            Files.createDirectories(chunkFile.getParent());
            OutputStream outputStream = Files.newOutputStream(chunkFile);
            ChunkFile chunk = new ChunkFile(outputStream);

            DocumentIterator documentIterator = documentSequence.iterator();
            Document document;
            Reader reader;
            WordReader wordReader;
            final MutableString word = new MutableString(), nonWord = new MutableString();

            // Scaffolding to demonstrate iteration over documents, fields, and terms.
            try (ChunkFile.FileScope fileScope = chunk.new FileScope()) {
                int documentId = 0;
                while ((document = documentIterator.nextDocument()) != null) {
                    System.out.println(document.title());

                    try (ChunkFile.DocumentScope documentScope = chunk.new DocumentScope(documentId)) {

                        // TODO: Don't hard-code fields.
                        for (int f = 0; f < 2; ++f)
                        {
                            System.out.println("  Field: " + f);

                            try (ChunkFile.StreamScope streamScope = chunk.new StreamScope(f)) {
                                Object content = document.content(f);
                                reader = (Reader) content;
                                wordReader = document.wordReader(f);
                                wordReader.setReader(reader);
                                while (wordReader.next(word, nonWord)) {
                                    String text = word.toString().toLowerCase();

                                    if (text.length() > 0) {
                                        System.out.print("    ");
                                        System.out.println(text);

                                        chunk.emit(text);
                                    }
                                    else {
                                        System.out.println("    Skipped zero-length word.");
                                    }
                                }
                            }
                        }
                    }

                    ++documentId;
                }
            }

            outputStream.close();
        }
    }
}
