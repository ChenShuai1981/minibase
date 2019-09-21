package org.apache.minibase;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

public class BlockCache {
    private static final Logger LOG = Logger.getLogger(BlockCache.class);

    private MiniBase miniBase;
    private LoadingCache<byte[], Optional<KeyValue>> cache;
    private AtomicLong totalGetReqs = new AtomicLong(0);
    private AtomicLong totalGetReqsThroughCache = new AtomicLong(0);

    public BlockCache(int cacheSize, MiniBase miniBase) {
        this.cache = CacheBuilder.newBuilder().maximumSize(cacheSize).build(new KeyValueCacheLoader());
        this.miniBase = miniBase;
    }

    public KeyValue get(byte[] key) {
        this.totalGetReqs.incrementAndGet();
        Optional<KeyValue> kv = null;
        try {
            kv = cache.get(key);
        } catch (ExecutionException e) {
            LOG.error("Failed to get value from cache by key: " + key, e);
        }
        if (!kv.isPresent()) {
            delete(key);
        }
        float cacheHitRate = this.totalGetReqsThroughCache.get() * 1.0f / this.totalGetReqs.get() * 1.0f;
        LOG.info("Cache hit rate: " + String.format("%.2f", cacheHitRate * 100f) + "%");
        return kv.orElse(null);
    }

    public void put(byte[] key, KeyValue kv) {
        cache.put(key, Optional.of(kv));
    }

    public void delete(byte[] key) {
        cache.invalidate(key);
    }

    class KeyValueCacheLoader extends CacheLoader<byte[], Optional<KeyValue>> {

        @Override
        public Optional<KeyValue> load(byte[] key) throws Exception {
            totalGetReqsThroughCache.incrementAndGet();
            Optional<KeyValue> result = Optional.ofNullable(null);
            try {
                result = miniBase.getByBloomFilter(key);
            } catch (IOException e) {
                LOG.error("Error when load key " + key + " to cache", e);
            }
            return result;
        }
    }
}
