package accord.api;

import accord.txn.Timestamp;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.ImmediateFuture;

/**
 * A collection of data to write to one or more stores
 *
 * TODO: support splitting so as to minimise duplication of data across shards
 */
public interface Write
{
    Future<?> SUCCESS = ImmediateFuture.success(null);
    Future<?> apply(Key key, Timestamp executeAt, DataStore store);
}
