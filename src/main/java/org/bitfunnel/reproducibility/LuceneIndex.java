package org.bitfunnel.reproducibility;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;

import java.io.IOException;
import java.nio.file.Paths;

public class LuceneIndex {
    // Lucene setup.
    // We use MMapDirectory instead of RAMDirectory because Lucene documentation recommends MMapDirectory for better
    // performance with "large" (> 100MB) indicies.
    // However, other people have observed minimal differences between the two:
    // http://blog.mikemccandless.com/2012/07/lucene-index-in-ram-with-azuls-zing-jvm.html
    // With our setup, ingestion is significantly slower and querying is marginally faster when using MMapDirectory.
    // The speedup in query speed is within the normal variance between runs whereas the slowdown in ingestion speed is
    // large and noticeable.
    Directory dir;

    public LuceneIndex(String basename) throws IOException {
        dir = new MMapDirectory(Paths.get(basename));
    }
}
