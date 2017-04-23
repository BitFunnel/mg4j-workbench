package org.bitfunnel.reproducibility;

//import com.martiansoftware.jsap.JSAPException;
//import com.martiansoftware.jsap.Parameter;
//import com.martiansoftware.jsap.SimpleJSAP;
//import it.unimi.di.big.mg4j.document.CompositeDocumentFactory;
//import it.unimi.di.big.mg4j.document.DocumentFactory;
//import it.unimi.di.big.mg4j.document.TRECDocumentCollection;
//import it.unimi.di.big.mg4j.document.TRECHeaderDocumentFactory;
//import it.unimi.dsi.fastutil.io.BinIO;
//
//import java.io.BufferedReader;
//import java.io.IOException;
//import java.io.InputStreamReader;
//import java.lang.reflect.InvocationTargetException;
//import java.util.Arrays;

import it.unimi.di.big.mg4j.document.*;

//import it.unimi.di.big.mg4j.document.PropertyBasedDocumentFactory.MetadataKeys;
import it.unimi.di.big.mg4j.util.MG4JClassParser;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.io.FastBufferedInputStream;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
//import it.unimi.dsi.fastutil.objects.ObjectArrays;
//import it.unimi.dsi.fastutil.objects.ObjectBigArrayBigList;
//import it.unimi.dsi.fastutil.objects.ObjectIterator;
//import it.unimi.dsi.fastutil.objects.Reference2ObjectArrayMap;
//import it.unimi.dsi.fastutil.objects.Reference2ObjectMap;
////import it.unimi.dsi.io.SegmentedInputStream;
////import it.unimi.dsi.logging.ProgressLogger;
//import it.unimi.dsi.util.Properties;

import java.io.BufferedReader;
//import java.io.FileInputStream;
//import java.io.FileNotFoundException;
import java.io.IOException;
//import java.io.InputStream;
import java.io.InputStreamReader;
//import java.io.ObjectInputStream;
//import java.io.ObjectOutputStream;
//import java.io.Serializable;
//import java.io.UnsupportedEncodingException;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
//import java.util.zip.GZIPInputStream;

//import org.apache.commons.io.IOUtils;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;

import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;
import com.martiansoftware.jsap.Switch;
import com.martiansoftware.jsap.UnflaggedOption;

// Step 1: build a collection
//     Main class: it.unimi.di.big.mg4j.document.TRECDocumentCollection
//     Program arguments: -f HtmlDocumentFactory -p encoding=iso-8859-1 d:\data\work\out2.collection d:\data\gov2\gx000\gx000\00.txt

// Step 2: build an index
//     Main class: it.unimi.di.big.mg4j.tool.IndexBuilder
//     Program arguments: --keep-batches --downcase -S d:\data\work\out2.collection d:\data\work\out2

// Step 3: run the web server
// Note: the following won't work because "d" is extracted as the basename.
//     it.unimi.di.big.mg4j.query.Query -h -i FileSystemItem -c d:\data\work\out2.collection d:\data\work\out2-text d:\data\work\out2-title
// Need to do
//     Main class: it.unimi.di.big.mg4j.query.Query
//     Working directory: d:\data\work
//     Program arguments: -h -i FileSystemItem -c out2.collection out2-text out2-title
// Then go to
//     http://localhost:4242/Query
//

// Step 4: run a single query for "dog" from the command line
//     Main class: it.unimi.di.big.mg4j.examples.RunQuery
//     Working directory: d:\data\work
//     Program arguments: out2 dog

// Sucessful command line invocations
// "C:\Program Files\Java\jdk1.8.0_131\bin\java" -cp D:\git\TestMG4J\out\production\TestMG4J;d:\git\TestMG4J\mg4j-big-deps/* com.company.Main --help
// "C:\Program Files\Java\jdk1.8.0_131\bin\java" -cp D:\git\TestMG4J\out\production\TestMG4J;d:\git\TestMG4J\mg4j-big-deps/* it.unimi.di.big.mg4j.document.FileSetDocumentCollection --help

// $x = Get-ChildItem "d:\data" -Recurse | where {$_.extension -eq ".7z"} | select FullName
// $y = (dir  | % { $_.fullname }) -join ' '
// ((dir).fullname) -join " "
// (dir | where {$_.extension -eq ".7z"} | select -first 10).fullname -join " "

// &java -cp D:\git\TestMG4J\out\production\TestMG4J`;d:\git\TestMG4J\mg4j-big-deps/`* it.unimi.di.big.mg4j.document.TRECDocumentCollection --help

// &java -cp D:\git\TestMG4J\out\production\TestMG4J`;d:\git\TestMG4J\mg4j-big-deps/`* it.unimi.di.big.mg4j.document.TRECDocumentCollection -z -f HtmlDocumentFactory -p encoding=iso-8859-1 d:\data\word\out.collection $x

// This one works. Note: should convert 7z files to tar.gz files so that -z option can be used. Otherwise will have too many file names for command line.
// &java -cp D:\git\TestMG4J\out\production\TestMG4J`;d:\git\TestMG4J\mg4j-big-deps/`* it.unimi.di.big.mg4j.document.TRECDocumentCollection -f HtmlDocumentFactory -p encoding=iso-8859-1 d:\data\work\out.collection D:\data\gov2\GX000\GX000\00.txt

// &java -cp D:\git\TestMG4J\out\production\TestMG4J`;d:\git\TestMG4J\mg4j-big-deps/`* it.unimi.di.big.mg4j.query.Query --help

public class Main {
    public static void main( final String[] arg ) throws IOException, JSAPException, InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {

        SimpleJSAP jsap = new SimpleJSAP(
                TRECDocumentCollection.class.getName(), "Saves a serialised TREC document collection based on a set of file names (which will be sorted lexicographically).",
                new Parameter[] {
                        new FlaggedOption( "factory", MG4JClassParser.getParser(), IdentityDocumentFactory.class.getName(), JSAP.NOT_REQUIRED, 'f', "factory", "A document factory with a standard constructor." ),
                        new FlaggedOption( "property", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'p', "property", "A 'key=value' specification, or the name of a property file" ).setAllowMultipleDeclarations( true ),
                        new Switch( "gzipped", 'z', "gzipped", "The files are gzipped." ),
                        new Switch( "unsorted", 'u', "unsorted", "Keep the file list unsorted." ),
                        new FlaggedOption( "bufferSize", JSAP.INTSIZE_PARSER, "64Ki", JSAP.NOT_REQUIRED, 'b', "buffer-size", "The size of an I/O buffer." ),
                        new UnflaggedOption( "collection", JSAP.STRING_PARSER, JSAP.REQUIRED, "The filename for the serialised collection." ),
                        new UnflaggedOption( "file", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, JSAP.GREEDY, "A list of files that will be indexed. If missing, a list of files will be read from standard input." )
                } );

        JSAPResult jsapResult = jsap.parse( arg );
        if ( jsap.messagePrinted() ) return;

        final DocumentFactory userFactory = PropertyBasedDocumentFactory.getInstance( jsapResult.getClass( "factory" ), jsapResult.getStringArray( "property" ) );

        String[] file = jsapResult.getStringArray( "file" );
        if ( file.length == 0 ) {
            final ObjectArrayList<String> files = new ObjectArrayList<String>();
            BufferedReader bufferedReader = new BufferedReader( new InputStreamReader( System.in ) );
            String s;
            while ( ( s = bufferedReader.readLine() ) != null ) files.add( s );
            file = files.toArray( new String[ 0 ] );
        }

        // To avoid problems with find and similar utilities, we sort the file names
        if ( !jsapResult.getBoolean( "unsorted" ) ) Arrays.sort( file );

        final DocumentFactory composite = CompositeDocumentFactory.getFactory( new TRECHeaderDocumentFactory(), userFactory );

        if ( file.length == 0 ) System.err.println( "WARNING: empty file set." );
        BinIO.storeObject( new TRECDocumentCollection( file, composite, jsapResult.getInt( "bufferSize" ), jsapResult.getBoolean( "gzipped" ) ), jsapResult.getString( "collection" ) );
    }
}
