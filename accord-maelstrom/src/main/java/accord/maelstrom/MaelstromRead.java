package accord.maelstrom;

import accord.api.*;
import accord.txn.Keys;
import accord.txn.Timestamp;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.ImmediateFuture;

public class MaelstromRead implements Read
{
    final Keys keys;

    public MaelstromRead(Keys keys)
    {
        this.keys = keys;
    }

    @Override
    public Future<Data> read(Key key, Timestamp executeAt, Store store)
    {
        MaelstromStore s = (MaelstromStore)store;
        MaelstromData result = new MaelstromData();
        result.put(key, s.get(key));
        return ImmediateFuture.success(result);
    }
}
