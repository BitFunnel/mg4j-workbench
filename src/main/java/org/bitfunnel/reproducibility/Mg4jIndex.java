package org.bitfunnel.reproducibility;

import it.unimi.di.big.mg4j.index.Index;
import it.unimi.di.big.mg4j.index.TermProcessor;
import it.unimi.dsi.fastutil.objects.Object2ReferenceOpenHashMap;
import org.apache.commons.configuration.ConfigurationException;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;


public class Mg4jIndex {
    final Index text;
    final Index title;
    final Object2ReferenceOpenHashMap<String,Index> indexMap;
    final Object2ReferenceOpenHashMap<String, TermProcessor> termProcessors;


    public Mg4jIndex(String basename) throws IllegalAccessException, InvocationTargetException, NoSuchMethodException, IOException, InstantiationException, URISyntaxException, ConfigurationException, ClassNotFoundException {
        // Load and configure the index.
        text = Index.getInstance( basename + "-text", true, true );
        title = Index.getInstance( basename + "-title", true, true );
//        text = Index.getInstance( basename + "-text?inmemory=1", true, true );
//        title = Index.getInstance( basename + "-title?inmemory=1", true, true );

        indexMap = new Object2ReferenceOpenHashMap<String,Index>(
                new String[] { "text", "title" }, new Index[] { text, title } );
        termProcessors = new Object2ReferenceOpenHashMap<String,TermProcessor>(
                new String[] { "text", "title" },
                new TermProcessor[] { text.termProcessor, title.termProcessor } );;
    }
}
