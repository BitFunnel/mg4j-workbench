echo Building mg4j-workbench
mvn package

echo Building collection . . .
java -cp target/mg4j-1.0-SNAPSHOT-jar-with-dependencies.jar ^
     it.unimi.di.big.mg4j.document.TRECDocumentCollection ^
     -f HtmlDocumentFactory -p encoding=iso-8859-1 d:\data\work\out2.collection d:\data\gov2\gx000\gx000\00.txt

type d:\data\work\out2-text.properties
type d:\data\work\out2-title.properties

echo Building BitFunnel chunks . . .
java -cp target/mg4j-1.0-SNAPSHOT-jar-with-dependencies.jar ^
     org.bitfunnel.reproducibility.GenerateBitFunnelChunks ^
      -S d:\data\work\out2.collection d:\data\work\out2.chunk

echo Building mg4j index . . .
java -cp target/mg4j-1.0-SNAPSHOT-jar-with-dependencies.jar ^
     it.unimi.di.big.mg4j.tool.IndexBuilder ^
      --keep-batches --downcase -S d:\data\work\out2.collection d:\data\work\out2

echo Measuring query performance . . .
java -cp target/mg4j-1.0-SNAPSHOT-jar-with-dependencies.jar ^
     org.bitfunnel.reproducibility.QueryPerformance ^
     d:\data\work\out2 d:\git\mg4j-workbench\data\small\queries10.txt

