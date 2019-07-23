package sandkev.simplecache;

import lombok.Builder;
import lombok.Data;

/**
 * Created by kevin on 03/04/2019.
 */
@Data
@Builder
public class ObjectDoc {
    String key;
    String parent;
    String dataType;
}
