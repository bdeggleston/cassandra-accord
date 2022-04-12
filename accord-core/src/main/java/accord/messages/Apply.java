package accord.messages;

import accord.api.Key;
import accord.api.Write;
import accord.local.Node;
import accord.local.Node.Id;
import accord.api.Result;
import accord.topology.Topologies;
import accord.txn.Dependencies;
import accord.txn.Timestamp;
import accord.txn.Writes;
import accord.txn.Txn;
import accord.txn.TxnId;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.UncheckedInterruptedException;

import java.util.concurrent.ExecutionException;

public class Apply extends TxnRequest
{
    public final TxnId txnId;
    public final Txn txn;
    // TODO: these only need to be sent if we don't know if this node has witnessed a Commit
    public final Dependencies deps;
    public final Timestamp executeAt;
    public final Writes writes;
    public final Result result;

    public Apply(Scope scope, TxnId txnId, Txn txn, Timestamp executeAt, Dependencies deps, Writes writes, Result result)
    {
        super(scope);
        this.txnId = txnId;
        this.txn = txn;
        this.deps = deps;
        this.executeAt = executeAt;
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

    public Apply(Node.Id to, Topologies topologies, TxnId txnId, Txn txn, Timestamp executeAt, Dependencies deps, Writes writes, Result result)
    {
        this(Scope.forTopologies(to, topologies, txn), txnId, txn, executeAt, deps, writes, result);
    }

    @Override
    public TxnId txnId()
    {
        return txnId;
    }

    @Override
    public Iterable<TxnId> depsIds()
    {
        return deps.txnIds();
    }

    @Override
    public Iterable<Key> keys()
    {
        return txn.keys();
    }

    public void process(Node node, Id replyToNode, ReplyContext replyContext)
    {
        node.mapReduceLocal(this, instance -> instance.command(txnId).apply(txn, deps, executeAt, writes, result), Apply::waitAndReduce);
    }

    @Override
    public MessageType type()
    {
        return MessageType.APPLY_REQ;
    }

    @Override
    public String toString()
    {
        return "Apply{" +
               "txnId: " + txnId +
               ", txn: " + txn +
               ", deps: " + deps +
               ", executeAt: " + executeAt +
               ", writes: " + writes +
               ", result: " + result +
               '}';
    }
}
