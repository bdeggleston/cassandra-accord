package accord.txn;

import accord.api.Write;
import accord.local.CommandStore;
import accord.topology.KeyRanges;
import com.google.common.base.Preconditions;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.FutureCombiner;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class Writes
{
    public final Timestamp executeAt;
    public final Keys keys;
    public final Write write;

    public Writes(Timestamp executeAt, Keys keys, Write write)
    {
        this.executeAt = executeAt;
        this.keys = keys;
        this.write = write;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Writes writes = (Writes) o;
        return executeAt.equals(writes.executeAt) && keys.equals(writes.keys) && write.equals(writes.write);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(executeAt, keys, write);
    }

    public Future<?> apply(CommandStore commandStore)
    {
        if (write == null)
            return Write.SUCCESS;

        KeyRanges ranges = commandStore.ranges().since(executeAt.epoch);
        if (ranges == null)
            return Write.SUCCESS;

        List<Future<?>> futures = keys.foldl(ranges, (key, accumulate) -> {
            if (commandStore.hashIntersects(key))
                accumulate.add(write.apply(key, commandStore, executeAt, commandStore.store()));
            return accumulate;
        }, new ArrayList<>());
        Preconditions.checkState(!futures.isEmpty());
        return futures.size() > 1 ? FutureCombiner.allOf(futures) : futures.get(0);
    }

    @Override
    public String toString()
    {
        return "TxnWrites{" +
               "executeAt:" + executeAt +
               ", keys:" + keys +
               ", write:" + write +
               '}';
    }
}
