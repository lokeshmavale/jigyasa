package com.jigyasa.dp.search.utils;

import lombok.RequiredArgsConstructor;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeoutException;

@RequiredArgsConstructor
public class DocIdOverlapLock {
    private final List<UniqueIdsPerRequest> list = new ArrayList<>();
    private final long timeoutMillis;

    public UniqueIdsPerRequest lock(final Set<String> keys) throws InterruptedException, TimeoutException {
        long timeout = this.timeoutMillis;

        List<UniqueIdsPerRequest> found;
        while (true) {
            synchronized (this.list) {
                found = findOverlap(keys);
                if (found.isEmpty()) {
                    UniqueIdsPerRequest uniques = new UniqueIdsPerRequest(keys);
                    this.list.add(uniques);
                    return uniques;
                }
            }
            for (int i = found.size() - 1; i >= 0; ) {
                UniqueIdsPerRequest ids = found.get(i);
                synchronized (ids) {
                    if (!ids.done) {
                        long currTime = System.currentTimeMillis();
                        ids.wait(timeout);
                        timeout -= System.currentTimeMillis() - currTime;
                        if (ids.done) {
                            i--;
                        }
                        if (timeout <= 0 && i >= 0) {
                            throw new TimeoutException("Dock Id overlap failed, Please retry again");
                        }
                    } else {
                        i--;
                    }
                }
            }
        }
    }

    private List<UniqueIdsPerRequest> findOverlap(Set<String> keys) {
        List<UniqueIdsPerRequest> found = new ArrayList<>();

        for (UniqueIdsPerRequest uniqueIdsPerRequest : list) {
            for (String key : uniqueIdsPerRequest.keys) {
                if (keys.contains(key)) {
                    found.add(uniqueIdsPerRequest);
                    break;
                }
            }
        }

        return found;
    }

    public void unlock(UniqueIdsPerRequest ids) {
        synchronized (this.list) {
            if (!this.list.remove(ids)) {
                throw new RuntimeException("Failed to remove unique ids from list");
            }
        }

        synchronized (ids) {
            ids.done = true;
            ids.notifyAll();
        }
    }

    @RequiredArgsConstructor
    public static class UniqueIdsPerRequest {
        private final Set<String> keys;
        private volatile boolean done;
    }
}
