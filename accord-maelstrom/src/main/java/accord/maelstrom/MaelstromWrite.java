package accord.maelstrom;

import accord.api.Key;
import accord.api.DataStore;
import accord.api.Write;
import accord.primitives.Timestamp;
import accord.local.CommandStore;
import accord.utils.Timestamped;
import org.apache.cassandra.utils.concurrent.Future;

import java.util.TreeMap;

public class MaelstromWrite extends TreeMap<Key, Value> implements Write
{
    @Override
    public Future<?> apply(Key key, CommandStore commandStore, Timestamp executeAt, DataStore store)
    {
        MaelstromStore s = (MaelstromStore) store;
        if (containsKey(key))
            s.data.merge(key, new Timestamped<>(executeAt, get(key)), Timestamped::merge);
        return SUCCESS;
    }
}
