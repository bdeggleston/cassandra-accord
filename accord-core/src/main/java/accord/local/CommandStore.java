package accord.local;

import accord.api.Agent;
import accord.api.Key;
import accord.api.Store;
import accord.local.CommandStores.StoreGroup;
import accord.topology.KeyRanges;
import accord.topology.Topology;
import accord.txn.Keys;
import accord.txn.Timestamp;
import accord.txn.TxnId;
import org.apache.cassandra.utils.concurrent.Future;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.cassandra.utils.concurrent.UncheckedInterruptedException;

/**
 * Single threaded internal shard of accord transaction metadata
 */
public abstract class CommandStore
{
    public interface Factory
    {
        CommandStore create(int generation,
                            int index,
                            int numShards,
                            Node.Id nodeId,
                            Function<Timestamp, Timestamp> uniqueNow,
                            Agent agent,
                            Store store,
                            KeyRanges ranges,
                            Supplier<Topology> localTopologySupplier);
    }

    private final int generation;
    private final int index;
    private final int numShards;
    private final Node.Id nodeId;
    private final Function<Timestamp, Timestamp> uniqueNow;
    private final Agent agent;
    private final Store store;
    private final KeyRanges ranges;
    private final Supplier<Topology> localTopologySupplier;


    public CommandStore(int generation,
                        int index,
                        int numShards,
                        Node.Id nodeId,
                        Function<Timestamp, Timestamp> uniqueNow,
                        Agent agent,
                        Store store,
                        KeyRanges ranges,
                        Supplier<Topology> localTopologySupplier)
    {
        this.generation = generation;
        this.index = index;
        this.numShards = numShards;
        this.nodeId = nodeId;
        this.uniqueNow = uniqueNow;
        this.agent = agent;
        this.store = store;
        this.ranges = ranges;
        this.localTopologySupplier = localTopologySupplier;
    }

    public abstract Command command(TxnId txnId);

    public abstract CommandsForKey commandsForKey(Key key);

    public Store store()
    {
        return store;
    }

    public Timestamp uniqueNow(Timestamp atLeast)
    {
        return uniqueNow.apply(atLeast);
    }

    public Agent agent()
    {
        return agent;
    }

    public Node.Id nodeId()
    {
        return nodeId;
    }

    public long epoch()
    {
        return localTopologySupplier.get().epoch();
    }

    public KeyRanges ranges()
    {
        return ranges;
    }

    public int generation()
    {
        return generation;
    }

    public int index()
    {
        return index;
    }

    public boolean hashIntersects(Key key)
    {
        return StoreGroup.keyIndex(key, numShards) == index;
    }

    public boolean intersects(Keys keys)
    {
        return keys.any(ranges, this::hashIntersects);
    }

    public boolean contains(Key key)
    {
        return ranges.contains(key);
    }

    public static void onEach(Collection<CommandStore> stores, TxnOperation scope, Consumer<? super CommandStore> consumer)
    {
        for (CommandStore store : stores)
            store.process(scope, consumer);
    }

    public abstract Future<Void> process(TxnOperation scope, Consumer<? super CommandStore> consumer);

    public abstract <T> Future<T> process(TxnOperation scope, Function<? super CommandStore, T> function);

    public abstract void shutdown();
}
