package accord.api;

import accord.txn.Keys;
import accord.txn.Timestamp;
import org.apache.cassandra.utils.concurrent.AsyncPromise;
import org.apache.cassandra.utils.concurrent.Future;

import java.util.List;
import java.util.function.BiConsumer;

/**
 * A read to be performed on potentially multiple shards, the inputs of which may be fed to a {@link Query}
 *
 * TODO: support splitting the read into per-shard portions
 */
public interface Read
{
    Keys keys();
    Future<Data> read(Key key, Timestamp executeAt, DataStore store);

    class Reducer extends AsyncPromise<Data> implements BiConsumer<Data, Throwable>
    {
        private Data result = null;
        private int pending = 0;

        public Reducer(List<Future<Data>> futures)
        {
            pending = futures.size();
            listen(futures);
        }

        private synchronized void listen(List<Future<Data>> futures)
        {
            for (int i=0, mi=futures.size(); i<mi; i++)
                futures.get(i).addCallback(this);
        }

        @Override
        public synchronized void accept(Data data, Throwable throwable)
        {
            if (isDone())
                return;

            if (throwable != null)
                tryFailure(throwable);

            result = result != null ? result.merge(data) : data;
            if (--pending == 0)
                trySuccess(result);
        }
    }
}
