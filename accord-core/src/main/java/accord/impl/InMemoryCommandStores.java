package accord.impl;

import accord.api.Agent;
import accord.api.Store;
import accord.local.CommandStore;
import accord.local.CommandStores;
import accord.local.Node;
import accord.topology.KeyRanges;
import accord.txn.Timestamp;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.UncheckedInterruptedException;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.ToLongBiFunction;

import static java.lang.Boolean.FALSE;

public abstract class InMemoryCommandStores extends CommandStores
{
    public InMemoryCommandStores(int numShards, Node.Id node, Function<Timestamp, Timestamp> uniqueNow, Agent agent, Store store)
    {
        super(numShards, node, uniqueNow, agent, store);
    }

    public static class Synchronized extends InMemoryCommandStores
    {
        public Synchronized(int numShards, Node.Id node, Function<Timestamp, Timestamp> uniqueNow, Agent agent, Store store)
        {
            super(numShards, node, uniqueNow, agent, store);
        }

        @Override
        protected CommandStore createCommandStore(int generation, int index, KeyRanges ranges)
        {
            return new InMemoryCommandStore.Synchronized(generation,
                                                         index,
                                                         numShards,
                                                         node,
                                                         uniqueNow,
                                                         agent,
                                                         store,
                                                         ranges,
                                                         this::getLocalTopology);
        }

        @Override
        protected <S, T> T mapReduce(ToLongBiFunction<StoreGroup, S> select, S scope, Function<? super CommandStore, T> map, BiFunction<T, T, T> reduce)
        {
            return groups.foldl(select, scope, (store, f, r, t) -> t == null ? f.apply(store) : r.apply(t, f.apply(store)), map, reduce, ignore -> null);
        }

        @Override
        protected <S> void forEach(ToLongBiFunction<StoreGroup, S> select, S scope, Consumer<? super CommandStore> forEach)
        {
            groups.foldl(select, scope, (store, f, r, t) -> { f.accept(store); return null; }, forEach, null, ignore -> FALSE);
        }
    }

    public static class SingleThread extends InMemoryCommandStores
    {
        public SingleThread(int numShards, Node.Id node, Function<Timestamp, Timestamp> uniqueNow, Agent agent, Store store)
        {
            super(numShards, node, uniqueNow, agent, store);
        }

        @Override
        protected CommandStore createCommandStore(int generation, int index, KeyRanges ranges)
        {
            return new InMemoryCommandStore.SingleThread(generation,
                                                         index,
                                                         numShards,
                                                         node,
                                                         uniqueNow,
                                                         agent,
                                                         store,
                                                         ranges,
                                                         this::getLocalTopology);
        }

        private <S, F, T> T mapReduce(ToLongBiFunction<StoreGroup, S> select, S scope, F f, StoreGroups.Fold<F, ?, List<Future<T>>> fold, BiFunction<T, T, T> reduce)
        {
            List<Future<T>> futures = groups.foldl(select, scope, fold, f, null, ArrayList::new);
            T result = null;
            for (Future<T> future : futures)
            {
                try
                {
                    T next = future.get();
                    if (result == null) result = next;
                    else result = reduce.apply(result, next);
                }
                catch (InterruptedException e)
                {
                    throw new UncheckedInterruptedException(e);
                }
                catch (ExecutionException e)
                {
                    throw new RuntimeException(e.getCause());
                }
            }
            return result;
        }

        @Override
        protected <S, T> T mapReduce(ToLongBiFunction<StoreGroup, S> select, S scope, Function<? super CommandStore, T> map, BiFunction<T, T, T> reduce)
        {
            return mapReduce(select, scope, map, (store, f, i, t) -> { t.add(store.process(f)); return t; }, reduce);
        }

        protected <S> void forEach(ToLongBiFunction<StoreGroup, S> select, S scope, Consumer<? super CommandStore> forEach)
        {
            mapReduce(select, scope, forEach, (store, f, i, t) -> { t.add(store.process(f)); return t; }, (Void i1, Void i2) -> null);
        }
    }
}
