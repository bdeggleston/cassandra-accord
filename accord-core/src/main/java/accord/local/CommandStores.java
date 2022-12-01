/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package accord.local;

import accord.api.*;
import accord.impl.InMemoryCommandStores;
import accord.local.CommandStore.RangesForEpoch;
import accord.primitives.AbstractKeys;
import accord.primitives.KeyRanges;
import accord.primitives.RoutingKeys;
import accord.topology.Topology;
import accord.utils.MapReduce;
import accord.utils.MapReduceConsume;

import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.IntStream;

import static accord.local.PreLoadContext.empty;

/**
 * Manages the single threaded metadata shards
 */
public abstract class CommandStores<S extends CommandStore>
{
    public interface Factory
    {
        CommandStores<?> create(int num,
                                Node node,
                                Agent agent,
                                DataStore store,
                                ProgressLog.Factory progressLogFactory);
    }

    private static class Supplier
    {
        private final NodeTimeService time;
        private final Agent agent;
        private final DataStore store;
        private final ProgressLog.Factory progressLogFactory;
        private final CommandStore.Factory shardFactory;
        private final int numShards;

        Supplier(NodeTimeService time, Agent agent, DataStore store, ProgressLog.Factory progressLogFactory, CommandStore.Factory shardFactory, int numShards)
        {
            this.time = time;
            this.agent = agent;
            this.store = store;
            this.progressLogFactory = progressLogFactory;
            this.shardFactory = shardFactory;
            this.numShards = numShards;
        }

        CommandStore create(int id, int generation, int shardIndex, RangesForEpoch rangesForEpoch)
        {
            return shardFactory.create(id, generation, shardIndex, numShards, time, agent, store, progressLogFactory, rangesForEpoch);
        }

        ShardedRanges createShardedRanges(int generation, long epoch, KeyRanges ranges, RangesForEpoch rangesForEpoch)
        {
            CommandStore[] newStores = new CommandStore[numShards];
            for (int i=0; i<numShards; i++)
                newStores[i] = create(generation * numShards + i, generation, i, rangesForEpoch);

            return new ShardedRanges(newStores, epoch, ranges);
        }
    }

    protected static class ShardedRanges
    {
        final CommandStore[] shards;
        final long[] epochs;
        final KeyRanges[] ranges;

        protected ShardedRanges(CommandStore[] shards, long epoch, KeyRanges ranges)
        {
            Preconditions.checkArgument(shards.length <= 64);
            this.shards = shards;
            this.epochs = new long[] { epoch };
            this.ranges = new KeyRanges[] { ranges };
        }

        private ShardedRanges(CommandStore[] shards, long[] epochs, KeyRanges[] ranges)
        {
            Preconditions.checkArgument(shards.length <= 64);
            this.shards = shards;
            this.epochs = epochs;
            this.ranges = ranges;
        }

        ShardedRanges withRanges(long epoch, KeyRanges ranges)
        {
            long[] newEpochs = Arrays.copyOf(this.epochs, this.epochs.length + 1);
            KeyRanges[] newRanges = Arrays.copyOf(this.ranges, this.ranges.length + 1);
            newEpochs[this.epochs.length] = epoch;
            newRanges[this.ranges.length] = ranges;
            return new ShardedRanges(shards, newEpochs, newRanges);
        }

        KeyRanges rangesForEpoch(long epoch)
        {
            int i = Arrays.binarySearch(epochs, epoch);
            if (i < 0) i = -2 -i;
            if (i < 0) return KeyRanges.EMPTY;
            return ranges[i];
        }

        KeyRanges rangesBetweenEpochs(long fromInclusive, long toInclusive)
        {
            if (fromInclusive > toInclusive)
                throw new IndexOutOfBoundsException();

            if (fromInclusive == toInclusive)
                return rangesForEpoch(fromInclusive);

            int i = Arrays.binarySearch(epochs, fromInclusive);
            if (i < 0) i = -2 - i;
            if (i < 0) i = 0;

            int j = Arrays.binarySearch(epochs, toInclusive);
            if (j < 0) j = -2 - j;
            if (i > j) return KeyRanges.EMPTY;

            KeyRanges result = ranges[i++];
            while (i <= j)
                result = result.union(ranges[i++]);
            return result;
        }

        KeyRanges rangesSinceEpoch(long epoch)
        {
            int i = Arrays.binarySearch(epochs, epoch);
            if (i < 0) i = Math.max(0, -2 -i);
            KeyRanges result = ranges[i++];
            while (i < ranges.length)
                result = ranges[i++].union(result);
            return result;
        }

        int indexForEpoch(long epoch)
        {
            int i = Arrays.binarySearch(epochs, epoch);
            if (i < 0) i = -2 -i;
            return i;
        }

        public long all()
        {
            return -1L >>> (64 - shards.length);
        }

        public long shards(AbstractKeys<?, ?> keys, long minEpoch, long maxEpoch)
        {
            long accumulate = 0L;
            for (int i = Math.max(0, indexForEpoch(minEpoch)), maxi = indexForEpoch(maxEpoch); i <= maxi ; ++i)
            {
                accumulate = keys.foldl(ranges[i], ShardedRanges::addKeyIndex, shards.length, accumulate, -1L);
            }
            return accumulate;
        }

        KeyRanges currentRanges()
        {
            return ranges[ranges.length - 1];
        }

        static long keyIndex(RoutingKey key, long numShards)
        {
            return Integer.toUnsignedLong(key.routingHash()) % numShards;
        }

        private static long addKeyIndex(int i, RoutingKey key, long numShards, long accumulate)
        {
            return accumulate | (1L << keyIndex(key, numShards));
        }
    }

    static class Snapshot
    {
        final ShardedRanges[] ranges;
        final Topology global;
        final Topology local;
        final int size;

        Snapshot(ShardedRanges[] ranges, Topology global, Topology local)
        {
            this.ranges = ranges;
            this.global = global;
            this.local = local;
            int size = 0;
            for (ShardedRanges group : ranges)
                size += group.shards.length;
            this.size = size;
        }
    }

    final Supplier supplier;
    volatile Snapshot current;

    private CommandStores(Supplier supplier)
    {
        this.supplier = supplier;
        this.current = new Snapshot(new ShardedRanges[0], Topology.EMPTY, Topology.EMPTY);
    }

    public CommandStores(int num, NodeTimeService time, Agent agent, DataStore store,
                         ProgressLog.Factory progressLogFactory, CommandStore.Factory shardFactory)
    {
        this(new Supplier(time, agent, store, progressLogFactory, shardFactory, num));
    }

    protected  <T> T mapReduceDirectUnsafe(Predicate<S> predicate, Function<S, T> map, BiFunction<T, T, T> reduce)
    {
        Snapshot snapshot = current;
        int idx = 0;
        T result = null;
        for (ShardedRanges ranges : snapshot.ranges)
        {
            for (CommandStore store : ranges.shards)
            {
                if (predicate.test((S) store))
                {
                    T value = map.apply((S) store);
                    if (idx++ == 0)
                        result = value;
                    else
                        result = reduce.apply(result, value);
                }
            }
        }

        return result;
    }

    public Topology local()
    {
        return current.local;
    }

    public Topology global()
    {
        return current.global;
    }

    private Snapshot updateTopology(Snapshot prev, Topology newTopology)
    {
        Preconditions.checkArgument(!newTopology.isSubset(), "Use full topology for CommandStores.updateTopology");

        long epoch = newTopology.epoch();
        if (epoch <= prev.global.epoch())
            return prev;

        Topology newLocalTopology = newTopology.forNode(supplier.time.id()).trim();
        KeyRanges added = newLocalTopology.ranges().difference(prev.local.ranges());
        KeyRanges subtracted = prev.local.ranges().difference(newLocalTopology.ranges());
//            for (ShardedRanges range : stores.ranges)
//            {
//                // FIXME: remove this (and the corresponding check in TopologyRandomizer) once lower bounds are implemented.
//                //  In the meantime, the logic needed to support acquiring ranges that we previously replicated is pretty
//                //  convoluted without the ability to jettison epochs.
//                Preconditions.checkState(!range.ranges.intersects(added));
//            }

        if (added.isEmpty() && subtracted.isEmpty())
            return new Snapshot(prev.ranges, newTopology, newLocalTopology);

        ShardedRanges[] result = new ShardedRanges[prev.ranges.length + (added.isEmpty() ? 0 : 1)];
        if (subtracted.isEmpty())
        {
            int newGeneration = prev.ranges.length;
            System.arraycopy(prev.ranges, 0, result, 0, newGeneration);
            result[newGeneration] = supplier.createShardedRanges(newGeneration, epoch, added, rangesForEpochFunction(newGeneration));
        }
        else
        {
            int i = 0;
            while (i < prev.ranges.length)
            {
                ShardedRanges ranges = prev.ranges[i];
                if (ranges.currentRanges().intersects(subtracted))
                    ranges = ranges.withRanges(newTopology.epoch(), ranges.currentRanges().difference(subtracted));
                result[i++] = ranges;
            }
            if (i < result.length)
                result[i] = supplier.createShardedRanges(i, epoch, added, rangesForEpochFunction(i));
        }

        return new Snapshot(result, newTopology, newLocalTopology);
    }

    private RangesForEpoch rangesForEpochFunction(int generation)
    {
        return new RangesForEpoch()
        {
            @Override
            public KeyRanges at(long epoch)
            {
                return current.ranges[generation].rangesForEpoch(epoch);
            }

            @Override
            public KeyRanges between(long fromInclusive, long toInclusive)
            {
                return current.ranges[generation].rangesBetweenEpochs(fromInclusive, toInclusive);
            }

            @Override
            public KeyRanges since(long epoch)
            {
                return current.ranges[generation].rangesSinceEpoch(epoch);
            }

            @Override
            public boolean owns(long epoch, RoutingKey key)
            {
                return current.ranges[generation].rangesForEpoch(epoch).contains(key);
            }

            @Override
            public boolean intersects(long epoch, AbstractKeys<?, ?> keys)
            {
                return at(epoch).intersects(keys);
            }
        };
    }

    public interface MapReduceAdapter<S extends CommandStore, Intermediate, Accumulator, O>
    {
        Accumulator allocate();
        Intermediate apply(MapReduce<? super SafeCommandStore, O> map, S commandStore, PreLoadContext context);
        Accumulator reduce(MapReduce<? super SafeCommandStore, O> reduce, Accumulator accumulator, Intermediate next);
        void consume(MapReduceConsume<?, O> consume, Intermediate reduced);
        Intermediate reduce(MapReduce<?, O> reduce, Accumulator accumulator);
    }

    // TODO: remove this and the adapter interface
    public static class AsyncMapReduceAdapter<O> implements MapReduceAdapter<CommandStore, AsyncChain<O>, List<AsyncChain<O>>, O>
    {
        private static final AsyncChain<?> SUCCESS = AsyncChains.success(null);
        private static final AsyncMapReduceAdapter<?> INSTANCE = new AsyncMapReduceAdapter<>();
        public static <O> AsyncMapReduceAdapter<O> instance() { return (AsyncMapReduceAdapter<O>) INSTANCE; }

        @Override
        public List<AsyncChain<O>> allocate()
        {
            return new ArrayList<>();
        }

        @Override
        public AsyncChain<O> apply(MapReduce<? super SafeCommandStore, O> map, CommandStore commandStore, PreLoadContext context)
        {
            return commandStore.submit(context, map);
        }

        @Override
        public List<AsyncChain<O>> reduce(MapReduce<? super SafeCommandStore, O> reduce, List<AsyncChain<O>> chains, AsyncChain<O> next)
        {
            chains.add(next);
            return chains;
        }

        @Override
        public void consume(MapReduceConsume<?, O> reduceAndConsume, AsyncChain<O> chain)
        {
            chain.begin(reduceAndConsume);
        }

        @Override
        public AsyncChain<O> reduce(MapReduce<?, O> reduce, List<AsyncChain<O>> futures)
        {
            if (futures.isEmpty())
                return (AsyncChain<O>) SUCCESS;
            return AsyncChains.reduce(futures, reduce::reduce);
        }
    }

    public AsyncChain<Void> forEach(Consumer<SafeCommandStore> forEach)
    {
        List<AsyncChain<Void>> list = new ArrayList<>();
        Snapshot snapshot = current;
        for (ShardedRanges ranges : snapshot.ranges)
        {
            for (CommandStore store : ranges.shards)
            {
                list.add(store.execute(empty(), forEach));
            }
        }
        return AsyncChains.reduce(list, (a, b) -> null);
    }

    public AsyncChain<Void> ifLocal(PreLoadContext context, RoutingKey key, long minEpoch, long maxEpoch, Consumer<SafeCommandStore> forEach)
    {
        return forEach(context, RoutingKeys.of(key), minEpoch, maxEpoch, forEach, false);
    }

    public AsyncChain<Void> forEach(PreLoadContext context, RoutingKey key, long minEpoch, long maxEpoch, Consumer<SafeCommandStore> forEach)
    {
        return forEach(context, RoutingKeys.of(key), minEpoch, maxEpoch, forEach, true);
    }

    public AsyncChain<Void> forEach(PreLoadContext context, AbstractKeys<?, ?> keys, long minEpoch, long maxEpoch, Consumer<SafeCommandStore> forEach)
    {
        return forEach(context, keys, minEpoch, maxEpoch, forEach, true);
    }

    private AsyncChain<Void> forEach(PreLoadContext context, AbstractKeys<?, ?> keys, long minEpoch, long maxEpoch, Consumer<SafeCommandStore> forEach, boolean matchesMultiple)
    {
        return this.mapReduce(context, keys, minEpoch, maxEpoch, new MapReduce<SafeCommandStore, Void>()
        {
            @Override
            public Void apply(SafeCommandStore in)
            {
                forEach.accept(in);
                return null;
            }

            @Override
            public Void reduce(Void o1, Void o2)
            {
                if (!matchesMultiple && minEpoch == maxEpoch)
                    throw new IllegalStateException();

                return null;
            }
        }, AsyncMapReduceAdapter.instance());
    }

    /**
     * See {@link #mapReduceConsume(PreLoadContext, AbstractKeys, long, long, MapReduceConsume)}
     */
    public <O> void mapReduceConsume(PreLoadContext context, RoutingKey key, long minEpoch, long maxEpoch, MapReduceConsume<? super SafeCommandStore, O> mapReduceConsume)
    {
        mapReduceConsume(context, RoutingKeys.of(key), minEpoch, maxEpoch, mapReduceConsume);
    }

    /**
     * Maybe asynchronously, {@code apply} the function to each applicable {@code CommandStore}, invoke {@code reduce}
     * on pairs of responses until only one remains, then {@code accept} the result.
     *
     * Note that {@code reduce} and {@code accept} are invoked by only one thread, and never concurrently with {@code apply},
     * so they do not require mutual exclusion.
     *
     * Implementations are expected to invoke {@link #mapReduceConsume(PreLoadContext, AbstractKeys, long, long, MapReduceConsume, MapReduceAdapter)}
     */
    public abstract <O> void mapReduceConsume(PreLoadContext context, AbstractKeys<?, ?> keys, long minEpoch, long maxEpoch, MapReduceConsume<? super SafeCommandStore, O> mapReduceConsume);
    public abstract <O> void mapReduceConsume(PreLoadContext context, IntStream commandStoreIds, MapReduceConsume<? super SafeCommandStore, O> mapReduceConsume);

    protected <T1, T2, O> void mapReduceConsume(PreLoadContext context, AbstractKeys<?, ?> keys, long minEpoch, long maxEpoch, MapReduceConsume<? super SafeCommandStore, O> mapReduceConsume,
                                                   MapReduceAdapter<? super S, T1, T2, O> adapter)
    {
        T1 reduced = mapReduce(context, keys, minEpoch, maxEpoch, mapReduceConsume, adapter);
        adapter.consume(mapReduceConsume, reduced);
    }

    protected <T1, T2, O> void mapReduceConsume(PreLoadContext context, IntStream commandStoreIds, MapReduceConsume<? super SafeCommandStore, O> mapReduceConsume,
                                                   MapReduceAdapter<? super S, T1, T2, O> adapter)
    {
        T1 reduced = mapReduce(context, commandStoreIds, mapReduceConsume, adapter);
        adapter.consume(mapReduceConsume, reduced);
    }

    protected <T1, T2, O> T1 mapReduce(PreLoadContext context, AbstractKeys<?, ?> keys, long minEpoch, long maxEpoch, MapReduce<? super SafeCommandStore, O> mapReduce,
                                       MapReduceAdapter<? super S, T1, T2, O> adapter)
    {
        T2 accumulator = adapter.allocate();
        for (ShardedRanges ranges : current.ranges)
        {
            long bits = ranges.shards(keys, minEpoch, maxEpoch);
            while (bits != 0)
            {
                int i = Long.numberOfTrailingZeros(bits);
                T1 next = adapter.apply(mapReduce, (S)ranges.shards[i], context);
                accumulator = adapter.reduce(mapReduce, accumulator, next);
                bits ^= Long.lowestOneBit(bits);
            }
        }
        return adapter.reduce(mapReduce, accumulator);
    }

    protected <T1, T2, O> T1 mapReduce(PreLoadContext context, IntStream commandStoreIds, MapReduce<? super SafeCommandStore, O> mapReduce,
                                       MapReduceAdapter<? super S, T1, T2, O> adapter)
    {
        // TODO: efficiency
        int[] ids = commandStoreIds.toArray();
        T2 accumulator = adapter.allocate();
        for (int id : ids)
        {
            T1 next = adapter.apply(mapReduce, (S)forId(id), context);
            accumulator = adapter.reduce(mapReduce, accumulator, next);
        }
        return adapter.reduce(mapReduce, accumulator);
    }

    public synchronized void updateTopology(Topology newTopology)
    {
        current = updateTopology(current, newTopology);
    }

    public synchronized void shutdown()
    {
        for (ShardedRanges group : current.ranges)
            for (CommandStore commandStore : group.shards)
                commandStore.shutdown();
    }

    CommandStore forId(int id)
    {
        ShardedRanges[] ranges = current.ranges;
        return ranges[id / supplier.numShards].shards[id % supplier.numShards];
    }

    @VisibleForTesting
    public CommandStore unsafeForKey(Key key)
    {
        ShardedRanges[] ranges = current.ranges;
        for (ShardedRanges group : ranges)
        {
            if (group.currentRanges().contains(key))
            {
                for (CommandStore store : group.shards)
                {
                    if (store.hashIntersects(key))
                        return store;
                }
            }
        }
        throw new IllegalArgumentException();
    }

}
