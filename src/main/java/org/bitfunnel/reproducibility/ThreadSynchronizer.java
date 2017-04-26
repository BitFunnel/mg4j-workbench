package org.bitfunnel.reproducibility;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


public class ThreadSynchronizer {
    int threadCount;
    final Lock lock = new ReentrantLock();
    final Condition waiting = lock.newCondition();
    long startTimeNs;

    public ThreadSynchronizer(int threadCount)
    {
        this.threadCount = threadCount;
    }

    public void waitForAllThreadsReady() throws InterruptedException {
        lock.lock();
        try {
            --threadCount;
            if (threadCount == 0) {
                startTimeNs = System.nanoTime();
                waiting.signalAll();
            }
            else {
                while (threadCount > 0) {
                    waiting.await();
                }
            }
        }
        finally {
            lock.unlock();
        }
    }
}
