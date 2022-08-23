package accord.messages;

import accord.api.Key;
import accord.primitives.Keys;
import accord.utils.VisibleForImplementation;
import accord.api.Write;
import accord.local.Node;
import accord.local.Node.Id;
import accord.api.Result;
import accord.topology.Topologies;
import accord.primitives.Deps;
import accord.primitives.Timestamp;
import accord.txn.Writes;
import accord.txn.Txn;
import accord.primitives.TxnId;
import com.google.common.collect.Iterables;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.UncheckedInterruptedException;

import java.util.Collections;
import java.util.concurrent.ExecutionException;

import static accord.messages.MessageType.APPLY_REQ;
import static accord.messages.MessageType.APPLY_RSP;

public class Apply extends TxnRequest
{
    public final TxnId txnId;
    public final Txn txn;
    public final Key homeKey;
    public final Timestamp executeAt;
    public final Deps deps;
    public final Writes writes;
    public final Result result;

    public Apply(Node.Id to, Topologies topologies, TxnId txnId, Txn txn, Key homeKey, Timestamp executeAt, Deps deps, Writes writes, Result result)
    {
        super(to, topologies, txn.keys());
        this.txnId = txnId;
        this.txn = txn;
        this.homeKey = homeKey;
        this.deps = deps;
        this.executeAt = executeAt;
        this.writes = writes;
        this.result = result;
    }

    @VisibleForImplementation
    public Apply(Keys scope, long waitForEpoch, TxnId txnId, Txn txn, Key homeKey, Timestamp executeAt, Deps deps, Writes writes, Result result)
    {
        super(scope, waitForEpoch);
        this.txnId = txnId;
        this.txn = txn;
        this.homeKey = homeKey;
        this.executeAt = executeAt;
        this.deps = deps;
        this.writes = writes;
        this.result = result;
    }

    static Future<?> waitAndReduce(Future<?> left, Future<?> right)
    {
        try
        {
            if (left != null) left.get();
            if (right != null) right.get();
        }
        catch (InterruptedException e)
        {
            throw new UncheckedInterruptedException(e);
        }
        catch (ExecutionException e)
        {
            throw new RuntimeException(e.getCause());
        }

        return Write.SUCCESS;
    }

    public void process(Node node, Id replyToNode, ReplyContext replyContext)
    {
        Key progressKey = node.trySelectProgressKey(txnId, txn.keys(), homeKey);
        node.mapReduceLocalSince(this, scope(), executeAt,
                                 instance -> instance.command(txnId).apply(txn, homeKey, progressKey, executeAt, deps, writes, result), Apply::waitAndReduce);
        // note, we do not also commit here if txnId.epoch != executeAt.epoch, as the scope() for a commit would be different
        node.reply(replyToNode, replyContext, ApplyOk.INSTANCE);
    }

    @Override
    public Iterable<TxnId> txnIds()
    {
        return Iterables.concat(Collections.singleton(txnId), deps.txnIds());
    }

    @Override
    public Iterable<Key> keys()
    {
        return txn.keys();
    }

    @Override
    public MessageType type()
    {
        return APPLY_REQ;
    }

    public static class ApplyOk implements Reply
    {
        public static final ApplyOk INSTANCE = new ApplyOk();
        public ApplyOk() {}

        @Override
        public String toString()
        {
            return "ApplyOk";
        }

        @Override
        public MessageType type()
        {
            return APPLY_RSP;
        }
    }

    @Override
    public String toString()
    {
        return "Apply{" +
               "txnId:" + txnId +
               ", txn:" + txn +
               ", deps:" + deps +
               ", executeAt:" + executeAt +
               ", writes:" + writes +
               ", result:" + result +
               '}';
    }
}
