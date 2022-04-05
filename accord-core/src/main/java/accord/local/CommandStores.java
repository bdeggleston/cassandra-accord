package accord.local;

import accord.api.Agent;
import accord.api.Key;
import accord.api.Store;
import accord.messages.TxnRequest;
import accord.topology.KeyRanges;
import accord.topology.Topology;
import accord.txn.Keys;
import accord.txn.Timestamp;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import java.util.*;
import java.util.function.*;

/**
 * Manages the single threaded metadata shards
 */
public abstract class CommandStores
{
    public interface Factory
    {
        CommandStores create(int num, Node.Id node, Function<Timestamp, Timestamp> uniqueNow, Agent agent, Store store);
    }

    // FIXME (rebase): rework forEach and mapReduce to not require these to be public
    public static class StoreGroup
    {
        final CommandStore[] stores;
        final KeyRanges ranges;

        public StoreGroup(CommandStore[] stores, KeyRanges ranges)
        {
            Preconditions.checkArgument(stores.length <= 64);
            this.stores = stores;
            this.ranges = ranges;
        }

        long all()
        {
            return -1L >>> (64 - stores.length);
        }

        long matches(Keys keys)
        {
            return keys.foldl(ranges, StoreGroup::addKeyIndex, stores.length, 0L, -1L);
        }

        long matches(TxnRequest request)
        {
            return matches(request.scope());
        }

        long matches(TxnRequest.Scope scope)
        {
            return matches(scope.keys());
        }

        static long keyIndex(Key key, long numShards)
        {
            return Integer.toUnsignedLong(key.keyHash()) % numShards;
        }

        private static long addKeyIndex(Key key, long numShards, long accumulate)
        {
            return accumulate | (1L << keyIndex(key, numShards));
        }
    }

    // FIXME (rebase): rework forEach and mapReduce to not require these to be public
    public static class StoreGroups
    {
        private static final StoreGroups EMPTY = new StoreGroups(new StoreGroup[0], Topology.EMPTY, Topology.EMPTY);
        final StoreGroup[] groups;
        final Topology global;
        final Topology local;
        final int size;

        public StoreGroups(StoreGroup[] groups, Topology global, Topology local)
        {
            this.groups = groups;
            this.global = global;
            this.local = local;
            int size = 0;
            for (StoreGroup group : groups)
                size += group.stores.length;
            this.size = size;
        }

        StoreGroups withNewTopology(Topology global, Topology local)
        {
            return new StoreGroups(groups, global, local);
        }

        public int size()
        {
            return size;
        }

        private <I1, I2, O> O foldl(int startGroup, long bitset, Fold<? super I1, ? super I2, O> fold, I1 param1, I2 param2, O accumulator)
        {
            int groupIndex = startGroup;
            StoreGroup group = groups[groupIndex];
            int offset = 0;
            while (true)
            {
                int i = Long.numberOfTrailingZeros(bitset) - offset;
                while (i < group.stores.length)
                {
                    accumulator = fold.fold(group.stores[i], param1, param2, accumulator);
                    bitset ^= Long.lowestOneBit(bitset);
                    i = Long.numberOfTrailingZeros(bitset) - offset;
                }

                if (++groupIndex == groups.length)
                    break;

                if (bitset == 0)
                    break;

                offset += group.stores.length;
                group = groups[groupIndex];
                if (offset + group.stores.length > 64)
                    break;
            }
            return accumulator;
        }

        // FIXME (rebase): rework forEach and mapReduce to not require these to be public
        public interface Fold<I1, I2, O>
        {
            O fold(CommandStore store, I1 i1, I2 i2, O accumulator);
        }

        // FIXME (rebase): rework forEach and mapReduce to not require these to be public
        public <S, I1, I2, O> O foldl(ToLongBiFunction<StoreGroup, S> select, S scope, Fold<? super I1, ? super I2, O> fold, I1 param1, I2 param2, IntFunction<? extends O> factory)
        {
            O accumulator = null;
            int startGroup = 0;
            while (startGroup < groups.length)
            {
                long bits = select.applyAsLong(groups[startGroup], scope);
                if (bits == 0)
                {
                    ++startGroup;
                    continue;
                }

                int offset = groups[startGroup].stores.length;
                int endGroup = startGroup + 1;
                while (endGroup < groups.length)
                {
                    StoreGroup group = groups[endGroup];
                    if (offset + group.stores.length > 64)
                        break;

                    bits += select.applyAsLong(group, scope) << offset;
                    offset += group.stores.length;
                    ++endGroup;
                }

                if (accumulator == null)
                    accumulator = factory.apply(Long.bitCount(bits));

                accumulator = foldl(startGroup, bits, fold, param1, param2, accumulator);
                startGroup = endGroup;
            }

            return accumulator;
        }
    }

    protected final Node.Id node;
    protected final Function<Timestamp, Timestamp> uniqueNow;
    protected final Agent agent;
    protected final Store store;
    protected final int numShards;

    protected volatile StoreGroups groups = StoreGroups.EMPTY;

    public CommandStores(int numShards, Node.Id node, Function<Timestamp, Timestamp> uniqueNow, Agent agent, Store store)
    {
        this.node = node;
        this.numShards = numShards;
        this.uniqueNow = uniqueNow;
        this.agent = agent;
        this.store = store;
    }

    protected abstract CommandStore createCommandStore(int generation, int index, KeyRanges ranges);

    protected Topology getLocalTopology()
    {
        return groups.local;
    }

    public synchronized void shutdown()
    {
        for (StoreGroup group : groups.groups)
            for (CommandStore commandStore : group.stores)
                commandStore.shutdown();
    }

    // FIXME (rebase): restore TxnRequest/TxnOperation functionality here
    protected abstract <S> void forEach(ToLongBiFunction<StoreGroup, S> select, S scope, Consumer<? super CommandStore> forEach);
    protected abstract <S, T> T mapReduce(ToLongBiFunction<StoreGroup, S> select, S scope, Function<? super CommandStore, T> map, BiFunction<T, T, T> reduce);

    public void forEach(Consumer<CommandStore> forEach)
    {
        forEach((s, i) -> s.all(), null, forEach);
    }

    public void forEach(Keys keys, Consumer<CommandStore> forEach)
    {
        forEach(StoreGroup::matches, keys, forEach);
    }

    public void forEach(TxnRequest request, Consumer<CommandStore> forEach)
    {
        forEach(StoreGroup::matches, request, forEach);
    }

    public <T> T mapReduce(TxnRequest request, Function<CommandStore, T> map, BiFunction<T, T, T> reduce)
    {
        return mapReduce(StoreGroup::matches, request, map, reduce);
    }

    public <T extends Collection<CommandStore>> T collect(Keys keys, IntFunction<T> factory)
    {
        return groups.foldl(StoreGroup::matches, keys, CommandStores::append, null, null, factory);
    }

    public <T extends Collection<CommandStore>> T collect(TxnRequest request, IntFunction<T> factory)
    {
        return groups.foldl(StoreGroup::matches, request, CommandStores::append, null, null, factory);
    }

    public <T extends Collection<CommandStore>> T collect(TxnRequest.Scope scope, IntFunction<T> factory)
    {
        return groups.foldl(StoreGroup::matches, scope, CommandStores::append, null, null, factory);
    }

    private static <T extends Collection<CommandStore>> T append(CommandStore store, Object ignore1, Object ignore2, T to)
    {
        to.add(store);
        return to;
    }

    public synchronized void updateTopology(Topology cluster)
    {
        Preconditions.checkArgument(!cluster.isSubset(), "Use full topology for CommandStores.updateTopology");

        StoreGroups current = groups;
        if (cluster.epoch() <= current.global.epoch())
            return;

        Topology local = cluster.forNode(node);
        KeyRanges added = local.ranges().difference(current.local.ranges());

        for (StoreGroup group : groups.groups)
        {
            // FIXME: remove this (and the corresponding check in TopologyRandomizer) once lower bounds are implemented.
            //  In the meantime, the logic needed to support acquiring ranges that we previously replicated is pretty
            //  convoluted without the ability to jettison epochs.
            Preconditions.checkState(!group.ranges.intersects(added));
        }

        if (added.isEmpty())
        {
            groups = groups.withNewTopology(cluster, local);
            return;
        }

        int newGeneration = current.groups.length;
        StoreGroup[] newGroups = new StoreGroup[current.groups.length + 1];
        CommandStore[] newStores = new CommandStore[numShards];
        System.arraycopy(current.groups, 0, newGroups, 0, current.groups.length);

        for (int i=0; i<numShards; i++)
            newStores[i] = createCommandStore(newGeneration, i, added);

        newGroups[current.groups.length] = new StoreGroup(newStores, added);

        groups = new StoreGroups(newGroups, cluster, local);
    }

    @VisibleForTesting
    public CommandStore unsafeForKey(Key key)
    {
        for (StoreGroup group : groups.groups)
        {
            if (group.ranges.contains(key))
            {
                for (CommandStore store : group.stores)
                {
                    if (store.hashIntersects(key))
                        return store;
                }
            }
        }
        throw new IllegalArgumentException();
    }
}
