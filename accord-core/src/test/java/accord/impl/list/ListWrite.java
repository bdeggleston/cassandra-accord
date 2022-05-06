package accord.impl.list;

import java.util.Arrays;
import java.util.TreeMap;
import java.util.stream.Collectors;

import accord.api.Key;
import accord.api.Store;
import accord.api.Write;
import accord.local.CommandStore;
import accord.txn.Timestamp;
import accord.utils.Timestamped;
import org.apache.cassandra.utils.concurrent.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ListWrite extends TreeMap<Key, int[]> implements Write
{
    private static final Logger logger = LoggerFactory.getLogger(ListWrite.class);

    @Override
    public Future<?> apply(Key key, CommandStore commandStore, Timestamp executeAt, Store store)
    {
        ListStore s = (ListStore) store;
        if (!containsKey(key))
            return SUCCESS;
        int[] data = get(key);
        s.data.merge(key, new Timestamped<>(executeAt, data), Timestamped::merge);
        logger.trace("WRITE on {} at {} key:{} -> {}", s.node, executeAt, key, data);
        return SUCCESS;
    }

    @Override
    public String toString()
    {
        return entrySet().stream()
                         .map(e -> e.getKey() + "=" + Arrays.toString(e.getValue()))
                         .collect(Collectors.joining(", ", "{", "}"));
    }
}
