package accord.local;

import java.util.*;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.stream.Stream;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;

import accord.api.Agent;
import accord.api.Key;
import accord.api.MessageSink;
import accord.api.Result;
import accord.api.ProgressLog;
import accord.api.Scheduler;
import accord.api.DataStore;
import accord.coordinate.Coordinate;
import accord.local.CommandStore.Factory;
import accord.messages.Callback;
import accord.messages.Request;
import accord.messages.Reply;
import accord.topology.KeyRange;
import accord.topology.KeyRanges;
import accord.topology.Shard;
import accord.topology.Shards;
import accord.topology.Topology;
import accord.txn.Ballot;
import accord.txn.Keys;
import accord.txn.Timestamp;
import accord.txn.Txn;
import accord.txn.TxnId;

public class Node
{
    public static class Id implements Comparable<Id>
    {
        public static final Id NONE = new Id(0);
        public static final Id MAX = new Id(Long.MAX_VALUE);

        public final long id;

        public Id(long id)
        {
            this.id = id;
        }

        @Override
        public int hashCode()
        {
            return Long.hashCode(id);
        }

        @Override
        public boolean equals(Object that)
        {
            return that instanceof Id && equals((Id) that);
        }

        public boolean equals(Id that)
        {
            return id == that.id;
        }

        @Override
        public int compareTo(Id that)
        {
            return Long.compare(this.id, that.id);
        }

        public String toString()
        {
            return Long.toString(id);
        }
    }

    public boolean isCoordinating(TxnId txnId, Ballot promised)
    {
        return promised.node.equals(id) && coordinating.containsKey(txnId);
    }

    public static int numCommandShards()
    {
        return 8; // TODO: make configurable
    }

    private final CommandStores commandStores;
    private final Id id;
    private final Topology cluster;
    private final MessageSink messageSink;
    private final Random random;

    private final LongSupplier nowSupplier;
    private final AtomicReference<Timestamp> now;
    private final Agent agent;

    // TODO: this really needs to be thought through some more, as it needs to be per-instance in some cases, and per-node in others
    private final Scheduler scheduler;

    private final Map<TxnId, CompletionStage<Result>> coordinating = new ConcurrentHashMap<>();

    public Node(Id id, Topology cluster, MessageSink messageSink, Random random, LongSupplier nowSupplier,
                Supplier<DataStore> dataSupplier, Agent agent, Scheduler scheduler, Function<? super Node, Function<? super CommandStore, ? extends ProgressLog>> retryLogFactory, Factory commandStoreFactory)
    {
        this.id = id;
        this.cluster = cluster;
        this.random = random;
        this.agent = agent;
        this.now = new AtomicReference<>(new Timestamp(nowSupplier.getAsLong(), 0, id));
        this.messageSink = messageSink;
        this.nowSupplier = nowSupplier;
        this.scheduler = scheduler;
        this.commandStores = new CommandStores(numCommandShards(), this, this::uniqueNow, agent, dataSupplier.get(), scheduler, retryLogFactory.apply(this), commandStoreFactory);
        this.commandStores.updateTopology(cluster.forNode(id));
    }

    public void shutdown()
    {
        commandStores.shutdown();
    }

    public Timestamp uniqueNow()
    {
        return now.updateAndGet(cur -> {
            // TODO: this diverges from proof; either show isomorphism or make consistent
            long now = nowSupplier.getAsLong();
            if (now > cur.real) return new Timestamp(now, 0, id);
            else return new Timestamp(cur.real, cur.logical + 1, id);
        });
    }

    public Timestamp uniqueNow(Timestamp atLeast)
    {
        if (now.get().compareTo(atLeast) < 0)
            now.accumulateAndGet(atLeast, (a, b) -> a.compareTo(b) < 0 ? new Timestamp(b.real, b.logical + 1, id) : a);

        return now.updateAndGet(cur -> {
            // TODO: this diverges from proof; either show isomorphism or make consistent
            long now = nowSupplier.getAsLong();
            if (now > cur.real) return new Timestamp(now, 0, id);
            else return new Timestamp(cur.real, cur.logical + 1, id);
        });
    }

    public long now()
    {
        return nowSupplier.getAsLong();
    }

    public Topology cluster()
    {
        return cluster;
    }

    public Stream<CommandStore> local(Keys keys)
    {
        return commandStores.forKeys(keys);
    }

    public Optional<CommandStore> local(Key key)
    {
        return local(Keys.of(key)).reduce((i1, i2) -> {
            throw new IllegalStateException("more than one instance encountered for key");
        });
    }

    // send to every node besides ourselves
    public void send(Shards shards, Request send)
    {
        Set<Id> contacted = new HashSet<>();
        shards.forEach(shard -> send(shard, send, contacted));
    }

    public void send(Shard shard, Request send)
    {
        shard.nodes.forEach(node -> messageSink.send(node, send));
    }

    private <T> void send(Shard shard, Request send, Set<Id> alreadyContacted)
    {
        shard.nodes.forEach(node -> {
            if (alreadyContacted.add(node))
                send(node, send);
        });
    }

    // send to every node besides ourselves
    public <T> void send(Shards shards, Request send, Callback<T> callback)
    {
        // TODO efficiency
        Set<Id> contacted = new HashSet<>();
        shards.forEach(shard -> send(shard, send, callback, contacted));
    }

    public <T> void send(Shard shard, Request send, Callback<T> callback)
    {
        shard.nodes.forEach(node -> send(node, send, callback));
    }

    private <T> void send(Shard shard, Request send, Callback<T> callback, Set<Id> alreadyContacted)
    {
        shard.nodes.forEach(node -> {
            if (alreadyContacted.add(node))
                send(node, send, callback);
        });
    }

    public <T> void send(Collection<Id> to, Request send)
    {
        for (Id dst: to)
            send(dst, send);
    }

    public <T> void send(Collection<Id> to, Request send, Callback<T> callback)
    {
        for (Id dst: to)
            send(dst, send, callback);
    }

    // send to a specific node
    public <T> void send(Id to, Request send, Callback<T> callback)
    {
        messageSink.send(to, send, callback);
    }

    // send to a specific node
    public void send(Id to, Request send)
    {
        messageSink.send(to, send);
    }

    public void reply(Id replyingToNode, long replyingToMessage, Reply send)
    {
        messageSink.reply(replyingToNode, replyingToMessage, send);
    }

    @VisibleForTesting
    public @Nullable Key trySelectHomeKey(Keys keys)
    {
        int i = commandStores.topology().ranges().findFirstIntersecting(keys);
        return i >= 0 ? keys.get(i) : null;
    }

    public Key selectHomeKey(Keys keys)
    {
        Key key = trySelectHomeKey(keys);
        return key != null ? key : selectRandomHomeKey();
    }

    public Key selectRandomHomeKey()
    {
        KeyRanges ranges = commandStores.topology().ranges();
        KeyRange range = ranges.get(ranges.size() == 1 ? 0 : random.nextInt(ranges.size()));
        return range.endInclusive() ? range.end() : range.start();
    }

    public CompletionStage<Result> coordinate(Txn txn)
    {
        TxnId txnId = new TxnId(uniqueNow());
        Key homeKey = trySelectHomeKey(txn.keys);
        if (homeKey == null)
        {
            homeKey = selectRandomHomeKey();
            txn = new Txn(txn.keys.with(homeKey), txn.read, txn.query, txn.update);
        }
        CompletionStage<Result> result = Coordinate.execute(this, txnId, txn, homeKey);
        coordinating.put(txnId, result);
        // TODO (now): if we fail, nominate another node to try instead
        result.whenComplete((success, fail) -> coordinating.remove(txnId));
        return result;
    }

    // TODO: encapsulate in Coordinate, so we can request that e.g. commits be re-sent?
    public CompletionStage<Result> recover(TxnId txnId, Txn txn, Key homeKey)
    {
        CompletionStage<Result> result = coordinating.get(txnId);
        if (result != null)
            return result;

        result = Coordinate.recover(this, txnId, txn, homeKey);
        coordinating.putIfAbsent(txnId, result);
        result.whenComplete((success, fail) -> {
            coordinating.remove(txnId);
            agent.onRecover(this, success, fail);
            // TODO (now): if we fail, nominate another node to try instead
        });
        return result;
    }

    public void receive(Request request, Id from, long messageId)
    {
        scheduler.now(() -> request.process(this, from, messageId));
    }

    public boolean isReplicaOf(Key key)
    {
        return commandStores.topology().ranges().contains(key);
    }

    public Scheduler scheduler()
    {
        return scheduler;
    }

    public Random random()
    {
        return random;
    }

    public Agent agent()
    {
        return agent;
    }

    public Id id()
    {
        return id;
    }

    @Override
    public String toString()
    {
        return "Node{" + id + '}';
    }
}
