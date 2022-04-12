package accord.messages;

import accord.api.Key;
import accord.local.Node;
import accord.local.Node.Id;
import accord.topology.Topologies;
import accord.txn.Timestamp;
import accord.txn.Dependencies;
import accord.txn.Txn;
import accord.txn.TxnId;

// TODO: CommitOk responses, so we can send again if no reply received? Or leave to recovery?
public class Commit extends ReadData
{
    public final boolean read;

    public Commit(Scope scope, TxnId txnId, Txn txn, Timestamp executeAt, Dependencies deps, boolean read)
    {
        super(scope, txnId, txn, deps, executeAt);
        this.read = read;
    }

    public Commit(Id to, Topologies topologies, TxnId txnId, Txn txn, Timestamp executeAt, Dependencies deps, boolean read)
    {
        this(Scope.forTopologies(to, topologies, txn), txnId, txn, executeAt, deps, read);
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

    public void process(Node node, Id from, ReplyContext replyContext)
    {
        node.mapReduceLocal(this, instance -> instance.command(txnId).commit(txn, deps, executeAt), Apply::waitAndReduce);
        if (read) super.process(node, from, replyContext);
    }

    @Override
    public MessageType type()
    {
        return MessageType.COMMIT_REQ;
    }

    @Override
    public String toString()
    {
        return "Commit{txnId: " + txnId +
               ", executeAt: " + executeAt +
               ", deps: " + deps +
               ", read: " + read +
               '}';
    }
}
