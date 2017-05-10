package org.bitfunnel.reproducibility;

import it.unimi.dsi.io.WordReader;
import it.unimi.dsi.lang.MutableString;

import java.io.IOException;
import java.io.Reader;


public class ChunkWordReader implements WordReader {
    Reader reader;
    boolean atEOF = false;

    @Override
    public boolean next(MutableString word, MutableString nonWord) throws IOException {
        word.length(0);
        nonWord.length(0);
        if (reader == null) {
            throw new IOException("ChunkWordReader.next(): no reader set.");
        }
        else if (atEOF) {
            // A prior call consumed the last word.
            return false;
        }
        else {
            int c = reader.read();
            if (c == -1) {
                // Stream always ends with a '\0'. We should never hit EOF.
                throw new IOException("ChunkWordReader.next(): unexpected EOF.");
            }
            else if (c == 0)
            {
                // We just hit the end-of-stream marker.
                atEOF = true;

                // TODO: Do we really want this check? This check reqires that we have one reader per stream,
                // instead of a single reader for the whole chunk.
                if (reader.read() != -1) {
                    throw new IOException("ChunkWordReader.next(): expected EOF, but found trailing content.");
                }

                return false;
            }
            else {
                // TODO: Fix this converstion to UTF-16. Issue #33.

                // We just hit the first letter of a word.
                // Convert to utf-16 and append to word.
                word.append((char)c);

                // Scan in the remainder of the word and its trailing '\0'.
                while (true) {
                    c = reader.read();
                    if (c == -1) {
                        // Stream always ends with a '\0'. We should never hit EOF.
                        throw new IOException("ChunkWordReader.next(): unexpected EOF.");
                    }
                    else if (c == 0)
                    {
                        // We've hit the trailing '\0' that marks the end of the word.
                        // Leave the nonWord empty.
                        break;
                    }
                    else {
                        // Convert to utf-16 and append to word.
                        word.append((char)c);
                    }
                }
                return true;
            }
        }
    }


    @Override
    public WordReader setReader(Reader reader)
    {
        this.reader = reader;
        atEOF = false;
        return this;
    }


    @Override
    public WordReader copy() {
        // Not implemented.
        System.out.println("ChunkWordReader.copy(): not implemented - returns null.");
        return null;
    }
}
