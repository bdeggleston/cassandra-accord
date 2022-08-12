package accord.coordinate;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;

import accord.api.Key;
import accord.api.Result;
import accord.coordinate.Invalidate.Outcome;
import accord.coordinate.tracking.AbstractQuorumTracker.QuorumShardTracker;
import accord.local.Node;
import accord.local.Node.Id;
import accord.messages.BeginInvalidate;
import accord.messages.BeginInvalidate.InvalidateNack;
import accord.messages.BeginInvalidate.InvalidateOk;
import accord.messages.BeginRecovery.RecoverReply;
import accord.messages.Callback;
import accord.messages.Commit;
import accord.topology.Shard;
import accord.txn.Ballot;
import accord.txn.Keys;
import accord.txn.Txn;
import accord.txn.TxnId;
import org.apache.cassandra.utils.concurrent.AsyncFuture;

import static accord.coordinate.Propose.Invalidate.proposeInvalidate;
import static accord.messages.BeginRecovery.RecoverOk.maxAcceptedOrLater;

public class Invalidate extends AsyncFuture<Outcome> implements Callback<RecoverReply>, BiConsumer<Result, Throwable>
{
    public enum Outcome { PREEMPTED, EXECUTED, INVALIDATED }

    final Node node;
    final Ballot ballot;
    final TxnId txnId;
    final Keys someKeys;
    final Key someKey;

    final List<Id> invalidateOksFrom = new ArrayList<>();
    final List<InvalidateOk> invalidateOks = new ArrayList<>();
    final QuorumShardTracker preacceptTracker;

    private Invalidate(Node node, Shard shard, Ballot ballot, TxnId txnId, Keys someKeys, Key someKey)
    {
        this.node = node;
        this.ballot = ballot;
        this.txnId = txnId;
        this.someKeys = someKeys;
        this.someKey = someKey;
        this.preacceptTracker = new QuorumShardTracker(shard);
    }

    public static Invalidate invalidate(Node node, TxnId txnId, Keys someKeys, Key someKey)
    {
        Ballot ballot = new Ballot(node.uniqueNow());
        Shard shard = node.topology().forEpochIfKnown(someKey, txnId.epoch);
        Invalidate invalidate = new Invalidate(node, shard, ballot, txnId, someKeys, someKey);
        node.send(shard.nodes, to -> new BeginInvalidate(txnId, someKey, ballot), invalidate);
        return invalidate;
    }

    @Override
    public synchronized void onSuccess(Id from, RecoverReply response)
    {
        if (isDone() || preacceptTracker.hasReachedQuorum())
            return;

        if (!response.isOK())
        {
            InvalidateNack nack = (InvalidateNack) response;
            if (nack.homeKey != null && nack.txn != null)
            {
                Key progressKey = node.trySelectProgressKey(txnId.epoch, nack.txn.keys, nack.homeKey);
                // TODO: consider limiting epoch upper bound we process this status for
                node.ifLocalSince(someKey, txnId, instance -> {
                    instance.command(txnId).preaccept(nack.txn, nack.homeKey, progressKey);
                    return null;
                });
            }
            else if (nack.homeKey != null)
            {
                // TODO: consider limiting epoch upper bound we process this status for
                node.ifLocalSince(someKey, txnId, instance -> {
                    instance.command(txnId).homeKey(nack.homeKey);
                    return null;
                });
            }
            trySuccess(Outcome.PREEMPTED);
            return;
        }

        InvalidateOk ok = (InvalidateOk) response;
        invalidateOks.add(ok);
        invalidateOksFrom.add(from);
        if (preacceptTracker.success(from))
            invalidate();
    }

    private void invalidate()
    {
        // first look to see if it has already been
        InvalidateOk acceptOrCommit = maxAcceptedOrLater(invalidateOks);
        if (acceptOrCommit != null)
        {
            switch (acceptOrCommit.status)
            {
                default: throw new IllegalStateException();
                case NotWitnessed:
                case PreAccepted:
                    throw new IllegalStateException("Should only have Accepted or later statuses here");
                case Accepted:
                    node.withEpoch(acceptOrCommit.executeAt.epoch, () -> {
                        Recover recover = new Recover(node, ballot, txnId, acceptOrCommit.txn, acceptOrCommit.homeKey);
                        recover.addCallback(this);

                        Set<Id> nodes = recover.tracker.topologies().copyOfNodes();
                        for (int i = 0 ; i < invalidateOks.size() ; ++i)
                        {
                            if (invalidateOks.get(i).executeAt != null)
                            {
                                recover.onSuccess(invalidateOksFrom.get(i), invalidateOks.get(i));
                                nodes.remove(invalidateOksFrom.get(i));
                            }
                        }
                        recover.start(nodes);
                    });
                    return;
                case AcceptedInvalidate:
                    break; // latest accept also invalidating, so we're on the same page and should finish our invalidation
                case Committed:
                case ReadyToExecute:
                    node.withEpoch(acceptOrCommit.executeAt.epoch, () -> {
                        Execute.execute(node, txnId, acceptOrCommit.txn, acceptOrCommit.homeKey, acceptOrCommit.executeAt,
                                        acceptOrCommit.deps, this);
                    });
                    return;
                case Executed:
                case Applied:
                    node.withEpoch(acceptOrCommit.executeAt.epoch, () -> {
                        Persist.persistAndCommit(node, txnId, acceptOrCommit.homeKey, acceptOrCommit.txn, acceptOrCommit.executeAt,
                                        acceptOrCommit.deps, acceptOrCommit.writes, acceptOrCommit.result);
                        trySuccess(Outcome.EXECUTED);
                    });
                    return;
                case Invalidated:
                    trySuccess(Outcome.INVALIDATED);
                    return;
            }
        }

        // if we have witnessed the transaction, but are able to invalidate, do we want to proceed?
        // Probably simplest to do so, but perhaps better for user if we don't.
        proposeInvalidate(node, ballot, txnId, someKey).addCallback((success, fail) -> {
            if (fail != null)
            {
                tryFailure(fail);
                return;
            }

            try
            {
                Commit.commitInvalidate(node, txnId, someKeys, txnId);
                node.forEachLocalSince(someKeys, txnId, instance -> {
                    instance.command(txnId).commitInvalidate();
                });
            }
            finally
            {
                trySuccess(Outcome.INVALIDATED);
            }
        });
    }

    @Override
    public void onFailure(Id from, Throwable failure)
    {
        if (isDone())
            return;

        if (preacceptTracker.failure(from))
            tryFailure(new Timeout(txnId, null));
    }

    @Override
    public void onCallbackFailure(Throwable failure)
    {
        tryFailure(failure);
    }

    @Override
    public void accept(Result result, Throwable fail)
    {
        if (fail != null)
        {
            if (fail instanceof Invalidated) trySuccess(Outcome.INVALIDATED);
            else tryFailure(fail);
        }
        else
        {
            node.agent().onRecover(node, result, fail);
            trySuccess(Outcome.EXECUTED);
        }
    }
}