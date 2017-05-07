package org.bitfunnel.reproducibility;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;


public abstract class QueryProcessorBase implements Runnable
{
    List<String> queries;
    int[] matchCounts;
    long[] parsingTimesInNS;
    long[] planningTimesInNS;
    long[] matchTimesInNS;
    boolean[] succeeded;

    ThreadSynchronizer warmupSynchronizer;
    AtomicInteger warmupQueriesRemaining;
    ThreadSynchronizer performanceSynchronizer;
    AtomicInteger performanceQueriesRemaining;
    ThreadSynchronizer finishSynchronizer;


    public QueryProcessorBase(QueryLogRunner runner)
    {
        this.queries = runner.queries;
        this.matchCounts = runner.matchCounts;

        this.parsingTimesInNS = runner.parsingTimesInNS;
        this.planningTimesInNS = runner.planningTimesInNS;
        this.matchTimesInNS = runner.matchTimesInNS;
        this.succeeded = runner.succeeded;
        this.warmupSynchronizer = runner.warmupSynchronizer;
        this.warmupQueriesRemaining = runner.warmupQueriesRemaining;
        this.performanceSynchronizer = runner.performanceSynchronizer;
        this.performanceQueriesRemaining = runner.performanceQueriesRemaining;
        this.finishSynchronizer = runner.finishSynchronizer;
    }


    @Override
    public void run() {
        try {
            // Process all queries once to "warm up the system".
            warmupSynchronizer.waitForAllThreadsReady();
            processLog(warmupQueriesRemaining);

            // Record performance measurements on final run.
            performanceSynchronizer.waitForAllThreadsReady();
            processLog(performanceQueriesRemaining);

            finishSynchronizer.waitForAllThreadsReady();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


    public void processLog(AtomicInteger queriesRemaining) {
        while (true) {
            int query = queriesRemaining.decrementAndGet();
            if (query < 0) {
                break;
            }

            int queryIndex = queries.size() - (query % queries.size()) - 1;

            processOneQuery(queryIndex, queries.get(queryIndex));
        }
    }

    public abstract void processOneQuery(int queryIndex, String query);
}
