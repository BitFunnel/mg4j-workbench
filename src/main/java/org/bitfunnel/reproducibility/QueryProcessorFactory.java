package org.bitfunnel.reproducibility;

import org.apache.commons.configuration.ConfigurationException;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;

public class QueryProcessorFactory {
    Mg4jIndex mg4jIndex = null;
    LuceneIndex luceneIndex = null;

    public QueryProcessorFactory(Mg4jIndex index){
        mg4jIndex = index;
    }

    public QueryProcessorFactory(LuceneIndex index) {
        luceneIndex = index;
    }

    Runnable createQueryProcessor(QueryLogRunner runner) throws IllegalAccessException, URISyntaxException, IOException, InstantiationException, NoSuchMethodException, ConfigurationException, InvocationTargetException, ClassNotFoundException {
        if (mg4jIndex != null) {
            return new Mg4jQueryProcessor(mg4jIndex, runner);
        }
        else if (luceneIndex != null) {
            return new LuceneQueryProcessor(luceneIndex, runner);
        }
        else {
            return null;
        }
    }

    String indexType() {
        if (mg4jIndex != null) {
            return "MG4j";
        }
        else if (luceneIndex != null) {
            return "Lucene";
        }
        else {
            return "Invalid";
        }
    }
}
