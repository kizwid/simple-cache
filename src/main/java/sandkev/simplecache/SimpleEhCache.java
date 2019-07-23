package sandkev.simplecache;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.config.units.MemoryUnit;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Created by kevin on 31/03/2019.
 */
public class SimpleEhCache implements SimpleCache<String, byte[]> {

    private static final Logger logger = LogManager.getLogger();

    private final CacheManager cacheManager;
    private final Cache<String, byte[]> cache;
    private final Indexer indexer;

    public SimpleEhCache(String storagePath, Indexer indexer) {

        this.cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
                .with(CacheManagerBuilder.persistence(new File(storagePath, "myData")))
                .withCache("objects",
                        CacheConfigurationBuilder.newCacheConfigurationBuilder(String.class, byte[].class,
                                ResourcePoolsBuilder.newResourcePoolsBuilder()
                                        .heap(1000000, EntryUnit.ENTRIES)
                                        .offheap(10, MemoryUnit.GB)
                                        .disk(100, MemoryUnit.GB, true)
                        )
                )
                .withCache("meta",
                        CacheConfigurationBuilder.newCacheConfigurationBuilder(String.class, String.class,
                                ResourcePoolsBuilder.newResourcePoolsBuilder()
                                        .heap(10000, EntryUnit.ENTRIES)
                                        .offheap(10, MemoryUnit.MB)
                                        .disk(100, MemoryUnit.MB, true)
                        )
                )
                .build(true);

        this.cache = cacheManager.getCache("objects", String.class, byte[].class);
        this.indexer = indexer;

    }

    public void put(String key, byte[] value, Map<String, String> meta) {

        if(cache.containsKey(key)){
            //this key already exists - since our data is immutable and since we don't want to store again (and invalidate the cached entry)
            //nothing to do here
            //todo: log that this data is already stored
            return;
        }

        //index this entry with its metadata
        indexer.addIndex(key, meta);

        cache.put(key, value);

    }

    public byte[] get(String key) {
        return cache.get(key);
    }

    public void clearBatch(String parent) throws IOException {

        Collection<ObjectDoc> docs = indexer.findKeys("Parent:" + parent, 0, 1000000);
        Set<String> keys = docs.stream().map(x -> x.getKey()).collect(Collectors.toSet());
        cache.removeAll(keys);

        //summarize the contents of the index
        //in terms of the number of each type of dataType
        //and the average hit ratios of each object/dataType

        //we also need to know which objects are shared between workflows

    }

    public boolean exists(String key) {
        return cache.containsKey(key);
    }

}
