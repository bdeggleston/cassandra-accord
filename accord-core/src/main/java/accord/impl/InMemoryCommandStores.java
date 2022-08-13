package accord.impl;

import accord.api.Agent;
import accord.api.DataStore;
import accord.api.ProgressLog;
import accord.local.CommandStore;
import accord.local.CommandStores;
import accord.local.Node;
import accord.local.TxnOperation;
import accord.messages.TxnRequest;
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

import static java.lang.Boolean.FALSE;

public abstract class InMemoryCommandStores extends CommandStores
{
    public InMemoryCommandStores(int num, Node node, Agent agent, DataStore store,
                                 ProgressLog.Factory progressLogFactory, CommandStore.Factory shardFactory)
    {
        super(num, node, agent, store, progressLogFactory, shardFactory);
    }

    public InMemoryCommandStores(Supplier supplier)
    {
        super(supplier);
    }

    public static class Synchronized extends InMemoryCommandStores
    {
        public Synchronized(int num, Node node, Agent agent, DataStore store, ProgressLog.Factory progressLogFactory)
        {
            super(num, node, agent, store, progressLogFactory, InMemoryCommandStore.Synchronized::new);
        }

        public Synchronized(Supplier supplier)
        {
            super(supplier);
        }

        @Override
        protected <S, T> T mapReduce(Select<S> select, S scope, long minEpoch, long maxEpoch, Function<? super CommandStore, T> map, BiFunction<T, T, T> reduce)
        {
            return foldl(select, scope, minEpoch, maxEpoch, (store, f, r, t) -> t == null ? f.apply(store) : r.apply(t, f.apply(store)), map, reduce, ignore -> null);
        }

        @Override
        protected <S> void forEach(Select<S> select, S scope, long minEpoch, long maxEpoch, Consumer<? super CommandStore> forEach)
        {
            foldl(select, scope, minEpoch, maxEpoch, (store, f, r, t) -> { f.accept(store); return null; }, forEach, null, ignore -> FALSE);
        }
    }

    public static class SingleThread extends InMemoryCommandStores
    {
        public SingleThread(int num, Node node, Agent agent, DataStore store, ProgressLog.Factory progressLogFactory)
        {
            this(num, node, agent, store, progressLogFactory, InMemoryCommandStore.SingleThread::new);
        }

        public SingleThread(int num, Node node, Agent agent, DataStore store, ProgressLog.Factory progressLogFactory, CommandStore.Factory shardFactory)
        {
            super(num, node, agent, store, progressLogFactory, shardFactory);
        }

        private <S, F, T> T mapReduce(Select<S> select, S scope, long minEpoch, long maxEpoch, F f, Fold<F, ?, List<Future<T>>> fold, BiFunction<T, T, T> reduce)
        {
            List<Future<T>> futures = foldl(select, scope, minEpoch, maxEpoch, fold, f, null, ArrayList::new);
            if (futures == null)
                return null;

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
        protected <S, T> T mapReduce(Select<S> select, S scope, long minEpoch, long maxEpoch, Function<? super CommandStore, T> map, BiFunction<T, T, T> reduce)
        {
            return mapReduce(select, scope, minEpoch, maxEpoch, map, (store, f, i, t) -> { t.add(store.process(f)); return t; }, reduce);
        }

        protected <S> void forEach(Select<S> select, S scope, long minEpoch, long maxEpoch, Consumer<? super CommandStore> forEach)
        {
            mapReduce(select, scope, minEpoch, maxEpoch, forEach, (store, f, i, t) -> { t.add(store.process(f)); return t; }, (Void i1, Void i2) -> null);
        }
    }

    public static class Debug extends InMemoryCommandStores.SingleThread
    {
        public Debug(int num, Node node, Agent agent, DataStore store, ProgressLog.Factory progressLogFactory)
        {
            super(num, node, agent, store, progressLogFactory, InMemoryCommandStore.SingleThreadDebug::new);
        }
    }

}
