# mg4j-workbench
Java tools for evaluating BitFunnel performance compared to an mg4j baseline.

## Building

### Windows

~~~
choco install java
choco install maven
mvn package
~~~

TODO: set JAVA_HOME?

### Linux

~~~
sudo add-apt-repository ppa:webupd8team/java
sudo apt-get update
sudo apt-get install oracle-java8-installer
sudo apt-get install maven
mvn package
~~~

TODO: set JAVA_HOME?

### OSX

Coming soon.

### IntelliJ

Import pom.xml.
Build -> Build Project

// TODO: Describe step-by-step.
// TODO: Add pictures.

## Creating an mg4j collection.

~~~
java -cp target/mg4j-1.0-SNAPSHOT-jar-with-dependencies.jar \
     it.unimi.di.big.mg4j.document.TRECDocumentCollection \
     -f HtmlDocumentFactory -p encoding=iso-8859-1 d:\data\work\out2.collection d:\data\gov2\gx000\gx000\00.txt
~~~

TODO: -z parameter for gz files.
TODO: substute <COLLECTION FILE> <GOV2 Files ...>

## Creating a BitFunnel chunk file from an mg4j collection.

~~~
java -cp target/mg4j-1.0-SNAPSHOT-jar-with-dependencies.jar \
     org.bitfunnel.reproducibility.GenerateBitFunnelChunks \
      -S <collection file> <chunk file>
~~~

## Building an mg4j index.

~~~
java -cp target/mg4j-1.0-SNAPSHOT-jar-with-dependencies.jar \
     it.unimi.di.big.mg4j.tool.IndexBuilder \
      --keep-batches --downcase -S d:\data\work\out2.collection d:\data\work\out2
~~~

TODO: Substitute <COLLECTION FILE> <BASENAME>
TODO: Add document filter parameter.


## Processing a query log.

~~~
java -cp target/mg4j-1.0-SNAPSHOT-jar-with-dependencies.jar \
     org.bitfunnel.reproducibility.QueryLogRunner \
     <index base name> <query log file> <output file> [-t threadCount]
~~~

## Exporting a Partitioned Elias-Fano Index

It is possible to export the mg4j index in a format usable by the
[Partitioned Elias-Fano Index](https://github.com/BitFunnel/partitioned_elias_fano) project.
The optional `--index` flag exports the index. The option `--queries` flag converts a
query log file for consumption by the Partitioned Elias-Fano Index. Two query files are
generated. The first has queries whose terms have been replaced by their integer term id values.
Queries with terms that are not in the index (and therefor don't have term id values) are
filtered out. The second query file has the plain text queries corresponding to those in the
file of term id queries.

~~~
java -cp target/mg4j-1.0-SNAPSHOT-jar-with-dependencies.jar \
     org.bitfunnel.reproducibility.IndexExporter \
     <index base name> <output base name> [--index] [--queries <query log file>]
~~~

## Filtering Query Logs
Note that one can use the `IndexExporter`, described in the previous section, to
generate a filtered query log that contains only those queries whose terms all
appear in the index. Just include the `--queries` parameter and remove the `--index`
parameter.