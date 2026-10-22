package io.github.hectorvent.floci.services.codeartifact;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.function.Function;

/**
 * Bounds the fan-out {@code listPackageVersions} needs when a sidecar has no single call that
 * returns every version's publish time at once: pypiserver answers with no timestamp field at
 * all (one {@code HEAD} per version), and Reposilite's per-version details page is the only way
 * to read a version's own file timestamps (one {@code GET} per version). Running those
 * sequentially means a one-result page still waits on every version in the package; running them
 * all at once risks opening as many sockets against the sidecar container as the package has
 * versions. Package-private: both callers live in this package, and the ordering/exception
 * guarantees below are shaped around what {@code listPackageVersions} specifically needs, not a
 * general-purpose utility.
 */
final class SidecarConcurrency {

    /**
     * How many per-item requests run against one sidecar container at once, no matter how many
     * {@code listPackageVersions} calls against that same container are in flight at the same
     * time: large enough that a package with a realistic number of versions sees a real speedup,
     * small enough that the container backing a sidecar can't be flooded with hundreds of
     * simultaneous connections.
     */
    private static final int MAX_CONCURRENT_REQUESTS = 8;

    /**
     * One {@link Semaphore} per sidecar container, shared across every {@code mapBounded} call for
     * that container so {@link #MAX_CONCURRENT_REQUESTS} holds in aggregate: a per-call-only limit
     * would let two simultaneous {@code listPackageVersions} calls against the same container each
     * open that many connections, doubling the real cap. The key is whatever identity a caller
     * already has for "which container": pypiserver's own {@code repositoryContainerId} (one
     * container per CodeArtifact repository), or a single shared constant for Reposilite (one
     * container for every Maven repository).
     */
    private static final Map<Object, Semaphore> SIDECAR_PERMITS = new ConcurrentHashMap<>();

    private static final ExecutorService EXECUTOR = Executors.newCachedThreadPool();

    private SidecarConcurrency() {
    }

    /**
     * Applies {@code perItem} to every element of {@code items}, in parallel up to {@link
     * #MAX_CONCURRENT_REQUESTS} at a time against {@code sidecarKey}'s container, and returns the
     * results in the same order as {@code items}. A single item's failure fails the whole call,
     * exactly as the original sequential loops this replaces did: the first failure's own
     * exception propagates (unwrapped from the {@link ExecutionException} every {@link Future}
     * wraps it in), the same error the caller would have seen from a plain sequential loop that
     * stopped on that item.
     *
     * <p>Every other item is then cancelled too: one still queued never starts at all, and one
     * already running is interrupted. Without this, a failure (or the caller thread being
     * interrupted) would leave the rest of the package's requests running against the sidecar in
     * the background even after the call itself has already failed.
     *
     * <p>Zero or one item skips the executor and the permit entirely and calls {@code perItem}
     * directly on the calling thread, since there is nothing to parallelize and nothing worth
     * bounding for it.
     */
    static <T, R> List<R> mapBounded(Object sidecarKey, List<T> items, Function<T, R> perItem) {
        if (items.size() <= 1) {
            List<R> result = new ArrayList<>(items.size());
            for (T item : items) {
                result.add(perItem.apply(item));
            }
            return result;
        }
        Semaphore permits = SIDECAR_PERMITS.computeIfAbsent(sidecarKey, key -> new Semaphore(MAX_CONCURRENT_REQUESTS));
        List<Future<R>> futures = new ArrayList<>(items.size());
        for (T item : items) {
            futures.add(EXECUTOR.submit(() -> runWithPermit(permits, perItem, item)));
        }
        boolean succeeded = false;
        try {
            List<R> results = new ArrayList<>(items.size());
            for (Future<R> future : futures) {
                results.add(await(future));
            }
            succeeded = true;
            return results;
        } finally {
            if (!succeeded) {
                for (Future<R> future : futures) {
                    future.cancel(true);
                }
            }
        }
    }

    private static <T, R> R runWithPermit(Semaphore permits, Function<T, R> perItem, T item)
            throws InterruptedException {
        permits.acquire();
        try {
            return perItem.apply(item);
        } finally {
            permits.release();
        }
    }

    private static <R> R await(Future<R> future) {
        try {
            return future.get();
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }
            throw new IllegalStateException(cause);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting on a sidecar request", e);
        }
    }
}
