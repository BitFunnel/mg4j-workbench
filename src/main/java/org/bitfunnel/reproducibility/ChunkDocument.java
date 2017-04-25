package org.bitfunnel.reproducibility;

import it.unimi.di.big.mg4j.document.Document;
import it.unimi.dsi.io.WordReader;
import it.unimi.dsi.lang.MutableString;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;


public class ChunkDocument implements Document {
    private InputStream input;

    private MutableString id;
    private MutableString title;
    private MutableString uri;

    // buffer holds UTF-8 content of all document streams.
    // Assumes that all documents are truncated at 256kb.
    // http://ir.dcs.gla.ac.uk/test_collections/gov2-summary.htm
    private static final int BUFFER_SIZE = 256 * 1024;
    private byte buffer[] = new byte[BUFFER_SIZE];

    // Index into buffer where next byte will be written.
    private int writeCursor = 0;

    /**
     * Class Stream represents a document stream (or field in mg4j parlance) with backing utf-8 data
     * in buffer[offset..offset + length - 1].
     */
    private class Stream {
        public Stream(int offset, int length) {
            this.offset = offset;
            this.length = length;
        }

        Reader reader() {
            return new InputStreamReader(new ByteArrayInputStream(buffer, offset, length), StandardCharsets.UTF_8);
        }

        int offset;
        int length;
    }

    // BitFunnel chunk specifies a 8-bit stream identifiers, so max stream id is 255.
    private static final int STREAM_COUNT = 256;

    // Mapping from stream id to (offset, length) of stream's backing data in buffer.
    Stream streams[] = new Stream[STREAM_COUNT];


    // Decide how this class throws parse errors.
    public ChunkDocument(InputStream input) throws IOException
    {
        this.input = input;

        // Parse the document header.
        parseHeader();

        // Parse each of the document's streams.
        while (tryParseStream());
    }


    private void parseHeader() throws IOException {
        readHexDigits(id, 8);
        expectByte('\0');

        title.append(id);
        uri.append("http://");
        uri.append(id);
    }


    private boolean tryParseStream() throws IOException {
        int id = readHexValue(2);

        // TODO: Consider changing chunk format to disallow multiple instances of a stream.
        if (streams[id] != null) {
            throw new IOException("ChunkDocument.tryParseStream(): encountered duplicate stream id.");
        }

        // Mark start of buffer.
        int offset = writeCursor;

        int c = input.read();
        if (c == -1) {
            throw new IOException("ChunkDocument.tryParseStream(): unexpected EOF.");
        }
        else if (c == 0) {
            // We've hit the end of the document.
            return false;
        }
        else {
            // Append the contents of this stream to the end of the buffer.
            int prev = -1;
            buffer[writeCursor++] = (byte)c;
            while (true)
            {
                c = input.read();
                if (c == -1){
                    throw new IOException("ChunkDocument.tryParseStream(): unexpected EOF.");
                }
                else {
                    buffer[writeCursor++] = (byte)c;
                    if (prev == 0 && c == 0) {
                        // We're at the end of the stream.
                        int length = writeCursor - offset;

                        // Make an entry in the streams table: id --> (offset, length)
                        streams[id] = new Stream(offset, length);

                        return true;
                    }
                    prev = c;
                }
            }
        }
    }


    /**
     * Copies {@code digitCount} consecutive hex digits from input to a MutableString.
     * @param digits
     * @param digitCount
     * @throws IOException
     */
    private void readHexDigits(MutableString digits, int digitCount) throws IOException {
        for (int i = 0; i < digitCount; ++i)
        {
            // Ok to read byte here since UTF-8 hex digits are in ASCII subset.
            int c = input.read();
            if ((c >= '0' && c<= '9') || (c >= 'a' && c <= 'f'))
                // Convert byte to UTF-16 char and append.
                digits.append((char)c);
            else {
                throw new IOException("ChunkDocument.readHexDigits(): expected hex digit.");
            }
        }
    }


    /**
     * Reads {@code digitCount} consecutive hex digits from input and converts to a non-negative integer.
     * WARNING: this class does not guard against int overflow.
     * @param digitCount
     * @return
     * @throws IOException
     */
    private int readHexValue(int digitCount) throws IOException {
        int value = 0;
        for (int i = 0; i < digitCount; ++i)
        {
            value *= 16;
            // Ok to read byte here since UTF-8 hex digits are in ASCII subset.
            int c = input.read();
            if (c >= '0' && c<= '9') {
                value += (c - '0');
            }
            else if (c >= 'a' && c <= 'f') {
                value += (c - 'a' + 10);
            }
            else {
                throw new IOException("ChunkDocument.readHexValue(): expected hex digit.");
            }
        }
        return value;
    }


    /**
     * Reads a single byte from the input stream and throws if the byte is not equal to an {@code expected} byte.
     * @param expected
     * @throws IOException
     */
    private void expectByte(int expected) throws IOException
    {
        int c = input.read();
        if (c != expected) {
            throw new IOException("ChunkDocument.expect(): unexpected character or EOF.");
        }
    }


    @Override
    public CharSequence title() {
        return title;
    }


    @Override
    public CharSequence uri() {
        return uri;
    }


    @Override
    public Object content(int i) throws IOException {
        Stream stream = streams[i];
        if (stream == null) {
            throw new IOException("ChunkDocument.content(): stream does not exist.");
        }
        return stream.reader();
    }


    public WordReader wordReader(int i) {
        return new ChunkWordReader();
    }


    @Override
    public void close() throws IOException {
        System.out.println("ChunkDocument.close()");
    }
}
