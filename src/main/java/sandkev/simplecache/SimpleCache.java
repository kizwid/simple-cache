package sandkev.simplecache;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

/**
 * Created by kevin on 31/03/2019.
 */
public interface SimpleCache<K,V> {

    enum ContextKeys{PARENT_ID, DATA_TYPE}

    void put(K key, V value, Map<String,String> context);
    V get(K key);
    void clearBatch(String parent) throws IOException;
    boolean exists(K key);


}
