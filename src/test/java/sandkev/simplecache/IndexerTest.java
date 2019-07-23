package sandkev.simplecache;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.junit.Test;

import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Created by kevin on 03/04/2019.
 */
public class IndexerTest {

    @Test
    public void canAddDocs() throws IOException {

        String storagePath = "target/test-classes/scratch";
        try(Indexer indexer = new Indexer(storagePath)){
            Map<String,String> meta = new HashMap<String, String>();
            meta.put(SimpleCache.ContextKeys.PARENT_ID.name(), "0");
            meta.put(SimpleCache.ContextKeys.DATA_TYPE.name(), "Trade");

            for (int n = 0; n < 1000; n++) {
                indexer.addIndex(String.valueOf(n),meta);
            }
            indexer.flush();
            assertEquals(1, indexer.findKeys("Key:0", 0, 100).size());
            assertEquals(2, indexer.findKeys("Key:1 or Key:2", 0, 100).size());
            assertEquals(1, indexer.findKeys("(Key:1) AND (DataType:Trade)", 0, 100).size());
            assertEquals(1, indexer.findKeys("+Key:1 +DataType:Trade", 0, 100).size());
            assertEquals(100, indexer.findKeys("Parent:0", 0, 100).size());
            assertEquals(1000, indexer.findKeys("Parent:0", 0, 1000).size());
            assertEquals(1000, indexer.findKeys("Parent:0", 0, 10000).size());
            assertEquals(100, indexer.findKeys("DataType:Trade", 0, 100).size());
            assertEquals(1000, indexer.findKeys("DataType:Trade", 0, 1000).size());
            assertEquals(1000, indexer.findKeys("DataType:Trade", 0, 10000).size());

        }

        Gson gson = new Gson();
        ArrayList fromJson = gson.fromJson(new FileReader("src/test/resources/docs/objects-1.json"), ArrayList.class);
//        ArrayList fromJson = gson.fromJson(new FileReader("src/test/resources/docs/objects-1.json"), new TypeToken<ArrayList<ObjectDoc>>(){}.getType());
        for (Object o : fromJson) {
            System.out.println(o);
        }
        System.out.println(fromJson);


    }

}