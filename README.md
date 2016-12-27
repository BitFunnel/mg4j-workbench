This is a quick "script" for ingesting BitFunnel chunk files and running a query log. This is currently intended for internal use only.

~~~
mvn package
java -cp target/lucene-runner-1.0-SNAPSHOT.jar org.bitfunnel.runner.LuceneRunner [manifest-file] [query-log] [num-threads]
~~~

