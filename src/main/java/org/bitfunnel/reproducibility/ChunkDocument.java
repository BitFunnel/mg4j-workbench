package org.bitfunnel.reproducibility;

import it.unimi.di.big.mg4j.document.Document;
import it.unimi.dsi.io.WordReader;
import it.unimi.dsi.lang.MutableString;

import java.io.*;
import java.nio.charset.StandardCharsets;


public class ChunkDocument implements Document {
    private PushbackInputStream input;

    private MutableString id = new MutableString();
    private MutableString title = new MutableString();
    private MutableString uri = new MutableString();

    // buffer holds UTF-8 content of all document streams.
    // Assumes that all documents are truncated at 256kb.
    // http://ir.dcs.gla.ac.uk/test_collections/gov2-summary.htm
    private static final int BUFFER_SIZE = 1024 * 1024;
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

    // TODO: Decide how this class throws parse errors.
    public ChunkDocument(PushbackInputStream input) throws IOException
    {
        this.input = input;

        // Parse the document header.
        parseHeader();

        // Parse each of the document's streams.
        while (tryParseStream()) {
            if (writeCursor >= 256 *1024) {
                System.out.println(String.format("======>>>> WARNING cursor: %d", writeCursor));
            }
        }
    }


    /**
     * Parses the document id from the chunk. Store the hex digits of the document id
     * and generates a title and uri based on the document id.
     *
     * <pre>
     * Grammar:
     *      Corpus: Document* End
     *      Document: DocumentId End Stream* End
     *      DocumentId: Hex2 Hex2 Hex2 Hex2 Hex2 Hex2 Hex2 Hex2
     *      (see rest of grammar in tryParseStream() documentation.
     * </pre>
     * @throws IOException
     */
    private void parseHeader() throws IOException {
        readHexDigits(id, 16);
        expectByte('\0');

        // For now just use the hex document id as the basis for the title and the uri.
        // TODO: Consider initializing title from contents of title stream.
        title.append(id);
        uri.append("localhost://");
        uri.append(id);
    }


    /**
     * Attempts to parse the utf-8 representation of a stream from input. If successful,
     * the utf-8 contents will be appended to buffer, a new entry will be made in the
     * streams array, and the method will return true. If, on entry, input contains no
     * more streams, the method returns false.
     *
     * <pre>
     * Grammar:
     *      Stream: StreamId End (Term End)* End
     *      StreamId: Hex2
     *      Hex2: Hex Hex
     *      Hex: [0123456789abcdef]
     *      Term: [^ \0] *
     *      End: \0
     * </pre>
     * @return true if a stream was parsed, otherwise false.
     * @throws IOException
     */
    private boolean tryParseStream() throws IOException {
        int c = input.read();
        if (c == -1) {
            // Document always ends with '\0', so we should never see EOF.
            throw new IOException("ChunkDocument.tryParseStream(): unexpected EOF.");
        }
        else if (c == 0) {
            // We've hit the end of the document.
            return false;
        }
        else {
            input.unread(c);

            // Each stream starts with a two-digit hex stream id, followed by '\0'.
            int id = readHexValue(2);

            // Move past '\0' that terminates the stream id.
            // As the parser moves forward, the variable, prev, keeps track of the previous byte in
            // order to detect the "End End" that delimits the end of a stream.
            int prev = input.read();
            if (prev != 0) {
                throw new IOException("ChunkDocument.tryParseStream(): expected zero after stream id.");
            }

            // Append the contents of this stream to the end of the buffer.
            // Scan past bytes that match "(Term End)*" End where Term is a sequence of non-zero utf-8 bytes
            // and End is the byte '\0'.

            // Mark start of this stream in the buffer.
            int offset = writeCursor;

            c = input.read();
            while (true)
            {
                if (c == -1){
                    throw new IOException("ChunkDocument.tryParseStream(): unexpected EOF.");
                }

                buffer[writeCursor++] = (byte)c;
                if (prev == 0 && c == 0) {
                    // We're at the end of the stream.
                    int length = writeCursor - offset;

                    // Make an entry in the streams table: id --> (offset, length)
                    streams[id] = new Stream(offset, length);

                    return true;
                }

                prev = c;
                c = input.read();
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
            throw new IOException(String.format("ChunkDocument.content(%d): stream does not exist.", i));
        }
        return stream.reader();
    }


    public WordReader wordReader(int i) {
        return new ChunkWordReader();
    }


    @Override
    public void close() throws IOException {
        // System.out.println("ChunkDocument.close()");
    }
}
