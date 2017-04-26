mvn package

# Building collection . . .
java -cp target/mg4j-1.0-SNAPSHOT-jar-with-dependencies.jar \
     it.unimi.di.big.mg4j.document.TRECDocumentCollection \
     -f HtmlDocumentFactory -p encoding=iso-8859-1 ~/dev/mg4j-tmp/out2.collection ~/dev/gov2/GX000/00.txt

# Building BitFunnel chunk files
java -cp target/mg4j-1.0-SNAPSHOT-jar-with-dependencies.jar \
     org.bitfunnel.reproducibility.GenerateBitFunnelChunks \
     -S ~/dev/mg4j-tmp/out2.collection ~/dev/mg4j-tmp/out2.chunk

# Building mg4j index from collection . . .
java -cp target/mg4j-1.0-SNAPSHOT-jar-with-dependencies.jar \
     it.unimi.di.big.mg4j.tool.IndexBuilder \
      --keep-batches --downcase -S ~/dev/mg4j-tmp/out2.collection ~/dev/mg4j-tmp/out2

# Building mg4j index from chunk . . .
java -cp target/mg4j-1.0-SNAPSHOT-jar-with-dependencies.jar \
     it.unimi.di.big.mg4j.tool.IndexBuilder \
     -o org.bitfunnel.reproducibility.ChunkDocumentSequence\(~/dev/mg4j-tmp/out2.chunk\) \
     ~/dev/mg4j-tmp/out2Chunk

## Index build info
cat ~/dev/mg4j-tmp/out2-text.properties
cat ~/dev/mg4j-tmp/out2Chunk-text.properties

cat ~/dev/mg4j-tmp/out2-title.properties
cat ~/dev/mg4j-tmp/out2Chunk-title.properties

# Measuring query performance multithreaded . . .
java -cp target/mg4j-1.0-SNAPSHOT-jar-with-dependencies.jar \
     org.bitfunnel.reproducibility.QueryLogRunner \
     ~/dev/mg4j-tmp/out2 ~/dev/mg4j-workbench/data/trec-terabyte/06.efficiency_topics.all ~/dev/mg4j-tmp/output.txt
