echo Building chunk00
java -cp target/mg4j-1.0-SNAPSHOT-jar-with-dependencies.jar ^
     it.unimi.di.big.mg4j.document.TRECDocumentCollection ^
     -f HtmlDocumentFactory -p encoding=iso-8859-1 d:\data\work3\00.collection d:\data\gov2\gx000\gx000\00.txt

java -cp target/mg4j-1.0-SNAPSHOT-jar-with-dependencies.jar ^
     org.bitfunnel.reproducibility.GenerateBitFunnelChunks ^
     -S d:\data\work3\00.collection d:\data\work3\00.chunk

echo Building chunk01
java -cp target/mg4j-1.0-SNAPSHOT-jar-with-dependencies.jar ^
     it.unimi.di.big.mg4j.document.TRECDocumentCollection ^
     -f HtmlDocumentFactory -p encoding=iso-8859-1 d:\data\work3\01.collection d:\data\gov2\gx000\gx000\01.txt

java -cp target/mg4j-1.0-SNAPSHOT-jar-with-dependencies.jar ^
     org.bitfunnel.reproducibility.GenerateBitFunnelChunks ^
     -S d:\data\work3\01.collection d:\data\work3\01.chunk

echo Index from chunk00
java -cp target/mg4j-1.0-SNAPSHOT-jar-with-dependencies.jar ^
     it.unimi.di.big.mg4j.tool.IndexBuilder ^
     -o org.bitfunnel.reproducibility.ChunkDocumentSequence(d:\data\work3\00.chunk) ^
     d:\data\work3\00

echo Index from chunk01
java -cp target/mg4j-1.0-SNAPSHOT-jar-with-dependencies.jar ^
     it.unimi.di.big.mg4j.tool.IndexBuilder ^
     -o org.bitfunnel.reproducibility.ChunkDocumentSequence(d:\data\work3\01.chunk) ^
     d:\data\work3\01

echo Create the manifest
dir /s/b d:\data\work3\*.chunk > d:\data\work3\manifest.txt

echo Index from chunk00 + chunk01
java -cp target/mg4j-1.0-SNAPSHOT-jar-with-dependencies.jar ^
     it.unimi.di.big.mg4j.tool.IndexBuilder ^
     -o org.bitfunnel.reproducibility.ChunkManifestDocumentSequence(d:\data\work3\manifest.txt) ^
     d:\data\work3\manifest
