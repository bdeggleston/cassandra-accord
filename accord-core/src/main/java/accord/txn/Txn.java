package accord.txn;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import accord.api.*;
import accord.local.*;
import org.apache.cassandra.utils.concurrent.Future;

public abstract class Txn
{
    public enum Kind { READ, WRITE, RECONFIGURE }

    public static class InMemory extends Txn
    {
        private final Kind kind;
        private final Keys keys;
        private final Read read;
        private final Query query;
        private final Update update;

        public InMemory(Keys keys, Read read, Query query)
        {
            this.kind = Kind.READ;
            this.keys = keys;
            this.read = read;
            this.query = query;
            this.update = null;
        }

        public InMemory(Keys keys, Read read, Query query, Update update)
        {
            this.kind = Kind.WRITE;
            this.keys = keys;
            this.read = read;
            this.update = update;
            this.query = query;
        }

        @Override
        public Kind kind()
        {
            return kind;
        }

        @Override
        public Keys keys()
        {
            return keys;
        }

        @Override
        public Read read()
        {
            return read;
        }

        @Override
        public Query query()
        {
            return query;
        }

        @Override
        public Update update()
        {
            return update;
        }
    }

    public abstract Kind kind();
    public abstract Keys keys();
    public abstract Read read();
    public abstract Query query();
    public abstract Update update();

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Txn txn = (Txn) o;
        return kind() == txn.kind()
                && keys().equals(txn.keys())
                && read().equals(txn.read())
                && query().equals(txn.query())
                && Objects.equals(update(), txn.update());
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(kind(), keys(), read(), query(), update());
    }

    public boolean isWrite()
    {
        switch (kind())
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
        return query().compute(data);
    }

    public Writes execute(Timestamp executeAt, Data data)
    {
        if (update() == null)
            return new Writes(executeAt, keys(), null);

        return new Writes(executeAt, keys(), update().apply(data));
    }

    public String toString()
    {
        return "{read:" + read().toString() + (update() != null ? ", update:" + update() : "") + '}';
    }

    public Read.ReadFuture read(Command command, Keys readKeys)
    {
        List<Future<Data>> futures = keys().foldl(command.commandStore().ranges().at(command.executeAt().epoch), (key, accumulate) -> {
            CommandStore commandStore = command.commandStore();
            if (!commandStore.hashIntersects(key))
                return accumulate;

            Future<Data> result = read().read(key, command.commandStore(), command.executeAt(), commandStore.store());
            accumulate.add(result);
            return accumulate;
        }, new ArrayList<>());
        return new Read.ReadFuture(readKeys, futures);
    }
}
