echo Building mg4j-workbench
mvn package

echo Building collection . . .
java -cp target/mg4j-1.0-SNAPSHOT-jar-with-dependencies.jar ^
     it.unimi.di.big.mg4j.document.TRECDocumentCollection ^
     -f HtmlDocumentFactory -p encoding=iso-8859-1 d:\data\work\out2.collection d:\data\gov2\gx000\gx000\00.txt

echo Building BitFunnel chunks . . .
java -cp target/mg4j-1.0-SNAPSHOT-jar-with-dependencies.jar ^
     org.bitfunnel.reproducibility.GenerateBitFunnelChunks ^
     -S d:\data\work\out2.collection d:\data\work\out2.chunk

echo Building mg4j index from collection . . .
java -cp target/mg4j-1.0-SNAPSHOT-jar-with-dependencies.jar ^
     it.unimi.di.big.mg4j.tool.IndexBuilder ^
      --keep-batches --downcase -S d:\data\work\out2.collection d:\data\work\out2

echo Building mg4j index from chunk . . .
java -cp target/mg4j-1.0-SNAPSHOT-jar-with-dependencies.jar ^
     it.unimi.di.big.mg4j.tool.IndexBuilder ^
     -o org.bitfunnel.reproducibility.ChunkDocumentSequence(d:\data\work\out2.chunk) ^
     d:\data\work\out2Chunk

type d:\data\work\out2-text.properties
type d:\data\work\out2Chunk-text.properties

type d:\data\work\out2-title.properties
type d:\data\work\out2Chunk-title.properties

echo Measuring query performance multithreaded . . .
java -cp target/mg4j-1.0-SNAPSHOT-jar-with-dependencies.jar ^
     org.bitfunnel.reproducibility.QueryLogRunner ^
     d:\data\work\out2 D:/git/mg4j-workbench/data/trec-terabyte/06.efficiency_topics.all d:\data\work\output.txt

echo Exporting a Partitioned Elias-Fano index . . .
java -cp target/mg4j-1.0-SNAPSHOT-jar-with-dependencies.jar ^
     org.bitfunnel.reproducibility.IndexExporter ^
     d:\data\junk\work\out2 d:\temp\export --index --queries  D:/git/mg4j-workbench/data/small/queries10.txt
