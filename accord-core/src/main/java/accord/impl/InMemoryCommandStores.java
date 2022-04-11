package accord.impl;

import accord.api.Agent;
import accord.api.Store;
import accord.local.CommandStore;
import accord.local.CommandStores;
import accord.local.Node;
import accord.topology.KeyRanges;
import accord.txn.Keys;
import accord.txn.Timestamp;
import accord.txn.Txn;

import java.util.function.Consumer;
import java.util.function.Function;

import static java.lang.Boolean.FALSE;

public abstract class InMemoryCommandStores extends CommandStores
{
    public InMemoryCommandStores(int numShards, Node.Id node, Function<Timestamp, Timestamp> uniqueNow, Agent agent, Store store)
    {
        super(numShards, node, uniqueNow, agent, store);
    }

    public static InMemoryCommandStores inMemory(Node node)
    {
        return (InMemoryCommandStores) node.commandStores();
    }

    public void forEachLocal(Keys keys, Consumer<? super CommandStore> forEach)
    {
        groups.foldl(StoreGroup::matches, keys, (store, f, r, t) -> { f.accept(store); return null; }, forEach, null, ignore -> FALSE);
    }

    public void forEachLocal(Txn txn, Consumer<? super CommandStore> forEach)
    {
        forEachLocal(txn.keys(), forEach);;
    }

    public void forEachLocal(Consumer<? super CommandStore> forEach)
    {
        groups.foldl((s, i) -> s.all(), numShards, (store, f, r, t) -> { f.accept(store); return null; }, forEach, null, ignore -> FALSE);
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
    }
}
