package accord.txn;

import accord.api.Write;
import accord.local.CommandStore;

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

    public void apply(CommandStore commandStore)
    {
        if (write == null)
            return;

        keys.foldl(commandStore.ranges(), (key, accumulate) -> {
            if (commandStore.hashIntersects(key))
                write.apply(key, executeAt, commandStore.store());
            return accumulate;
        }, null);
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
