# mg4j-workbench
Java tools for evaluating BitFunnel performance compared to an mg4j baseline.

## Building

### Windows

~~~
choco install java
choco install maven
mvn package
~~~

TODO: set JAVA_HOME

### Linux

~~~
sudo add-apt-repository ppa:webupd8team/java
sudo apt-get update
sudo apt-get install oracle-java8-installer
sudo apt-get install maven
mvn package
~~~

TODO: set JAVA_HOME

### IntelliJ

Import pom.xml.
Build -> Build Project

## Creating an mg4j collection.

Comming soon.

## Creating a BitFunnel chunk file from an mg4j collection.

### Linux

~~~
java -cp target/mg4j-1.0-SNAPSHOT-jar-with-dependencies.jar org.bitfunnel.reproducibility.GenerateBitFunnelChunks -S <collection file> <chunk file>
~~~

### Windows

~~~
java -cp target/mg4j-1.0-SNAPSHOT-jar-with-dependencies.jar org.bitfunnel.reproducibility.GenerateBitFunnelChunks -S <collection file> <chunk file>
~~~

## Processing a query log.

Coming soon.