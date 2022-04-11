package accord.txn;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

import accord.api.*;
import accord.local.*;
import org.apache.cassandra.utils.concurrent.Future;

public class Txn
{
    enum Kind { READ, WRITE, RECONFIGURE }

    final Kind kind;
    public final Keys keys;
    public final Read read;
    public final Query query;
    public final Update update;

    public Txn(Keys keys, Read read, Query query)
    {
        this.kind = Kind.READ;
        this.keys = keys;
        this.read = read;
        this.query = query;
        this.update = null;
    }

    public Txn(Keys keys, Read read, Query query, Update update)
    {
        this.kind = Kind.WRITE;
        this.keys = keys;
        this.read = read;
        this.update = update;
        this.query = query;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Txn txn = (Txn) o;
        return kind == txn.kind
                && keys.equals(txn.keys)
                && read.equals(txn.read)
                && query.equals(txn.query)
                && Objects.equals(update, txn.update);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(kind, keys, read, query, update);
    }

    public boolean isWrite()
    {
        switch (kind)
        {
            default:
                throw new IllegalStateException();
            case READ:
                return false;
            case WRITE:
            case RECONFIGURE:
                return true;
        }
    }

    public Result result(Data data)
    {
        return query.compute(data);
    }

    public Writes execute(Timestamp executeAt, Data data)
    {
        if (update == null)
            return new Writes(executeAt, keys, null);

        return new Writes(executeAt, keys, update.apply(data));
    }

    public Keys keys()
    {
        return keys;
    }

    public String toString()
    {
        return "read:" + read.toString() + (update != null ? ", update:" + update : "");
    }

    public Future<Data> read(Command command, Keys keyScope)
    {
        List<Future<Data>> futures = keyScope.foldl(command.commandStore().ranges(), (key, accumulate) -> {
            CommandStore commandStore = command.commandStore();
            if (!commandStore.hashIntersects(key))
                return accumulate;

            Future<Data> result = read.read(key, command.executeAt(), commandStore.store());
            accumulate.add(result);
            return accumulate;
        }, new ArrayList<>());
        return new Read.Reducer(futures);
    }

    public Timestamp maxConflict(CommandStore commandStore)
    {
        return maxConflict(commandStore, keys());
    }

    public Stream<Command> conflictsMayExecuteBefore(CommandStore commandStore, Timestamp mayExecuteBefore)
    {
        return keys().stream().flatMap(key -> {
            CommandsForKey forKey = commandStore.commandsForKey(key);
            return Stream.concat(
            forKey.uncommitted().before(mayExecuteBefore),
            // TODO: only return latest of Committed?
            forKey.committedByExecuteAt().before(mayExecuteBefore)
            );
        });
    }

    public Stream<Command> uncommittedStartedBefore(CommandStore commandStore, TxnId startedBefore)
    {
        return keys().stream().flatMap(key -> {
            CommandsForKey forKey = commandStore.commandsForKey(key);
            return forKey.uncommitted().before(startedBefore);
        });
    }

    public Stream<Command> committedStartedBefore(CommandStore commandStore, TxnId startedBefore)
    {
        return keys().stream().flatMap(key -> {
            CommandsForKey forKey = commandStore.commandsForKey(key);
            return forKey.committedById().before(startedBefore);
        });
    }

    public Stream<Command> uncommittedStartedAfter(CommandStore commandStore, TxnId startedAfter)
    {
        return keys().stream().flatMap(key -> {
            CommandsForKey forKey = commandStore.commandsForKey(key);
            return forKey.uncommitted().after(startedAfter);
        });
    }

    public Stream<Command> committedExecutesAfter(CommandStore commandStore, TxnId startedAfter)
    {
        return keys().stream().flatMap(key -> {
            CommandsForKey forKey = commandStore.commandsForKey(key);
            return forKey.committedByExecuteAt().after(startedAfter);
        });
    }

    public void register(CommandStore commandStore, Command command)
    {
        assert commandStore == command.commandStore();
        keys().forEach(key -> commandStore.commandsForKey(key).register(command));
    }

    protected Timestamp maxConflict(CommandStore commandStore, Keys keys)
    {
        return keys.stream()
                   .map(commandStore::commandsForKey)
                   .map(CommandsForKey::max)
                   .max(Comparator.naturalOrder())
                   .orElse(Timestamp.NONE);
    }

}
