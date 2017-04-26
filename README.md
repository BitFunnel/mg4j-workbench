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
     <index base name> <query log file>
~~~

