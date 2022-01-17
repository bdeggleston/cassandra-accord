package accord.coordinate;

import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;

import com.google.common.base.Preconditions;

import accord.api.Key;
import accord.local.Node;
import accord.local.Status;
import accord.messages.CheckStatus;
import accord.messages.CheckStatus.CheckStatusOk;
import accord.messages.CheckStatus.CheckStatusOkFull;
import accord.messages.CheckStatus.IncludeInfo;
import accord.topology.Shard;
import accord.txn.Ballot;
import accord.txn.Txn;
import accord.txn.TxnId;

/**
 * A result of null indicates the transaction is globally persistent
 * A result of CheckStatusOk indicates the maximum status found for the transaction, which may be used to assess progress
 */
public class MaybeRecover extends CheckShardProgress implements BiConsumer<Object, Throwable>
{
    MaybeRecover(Node node, TxnId txnId, Txn txn, Key homeKey, Shard homeShard, Status knownStatus, Ballot knownPromised, boolean knownPromiseHasBeenAccepted, byte includeInfo)
    {
        super(node, txnId, txn, homeKey, homeShard, knownStatus, knownPromised, knownPromiseHasBeenAccepted, includeInfo);
    }

    @Override
    public void accept(Object unused, Throwable fail)
    {
        if (fail != null) completeExceptionally(fail);
        else complete(null);
    }

    // TODO (now): invoke from {node} so we may have mutual exclusion with other attempts to recover or coordinate
    public static CompletionStage<CheckStatusOk> maybeRecover(Node node, TxnId txnId, Txn txn, Key homeKey, Shard homeShard,
                                                              Status knownStatus, Ballot knownPromised, boolean knownPromiseHasBeenAccepted)
    {
        return maybeRecover(node, txnId, txn, homeKey, homeShard, knownStatus, knownPromised, knownPromiseHasBeenAccepted, (byte)0);
    }

    private static CompletionStage<CheckStatusOk> maybeRecover(Node node, TxnId txnId, Txn txn, Key homeKey, Shard homeShard,
                                                               Status knownStatus, Ballot knownPromised, boolean knownPromiseHasBeenAccepted, byte includeInfo)
    {
        Preconditions.checkArgument(node.isReplicaOf(homeKey));
        MaybeRecover maybeRecover = new MaybeRecover(node, txnId, txn, homeKey, homeShard, knownStatus, knownPromised, knownPromiseHasBeenAccepted, includeInfo);
        maybeRecover.start();
        return maybeRecover;
    }

    void onSuccessCriteriaOrExhaustion()
    {
        switch (max.status)
        {
            default: throw new AssertionError();
            case NotWitnessed:
            case PreAccepted:
            case Accepted:
            case Committed:
            case ReadyToExecute:
                if (hasMadeProgress())
                {
                    complete(max);
                }
                else
                {
                    node.recover(txnId, txn, key)
                        .whenComplete(this);
                }
                break;

            case Executed:
            case Applied:
                if (max.hasExecutedOnAllShards)
                {
                    // TODO: persist this knowledge locally?
                    complete(null);
                }
                else if (max instanceof CheckStatusOkFull)
                {
                    CheckStatusOkFull full = (CheckStatusOkFull) max;
                    Persist.persist(node, node.cluster().forKeys(txn.keys), txnId, key, txn, full.executeAt, full.deps, full.writes, full.result)
                           .whenComplete(this);
                }
                else
                {
                    maybeRecover(node, txnId, txn, key, tracker.shard, max.status, max.promised, max.accepted.equals(max.promised), IncludeInfo.all())
                    .whenComplete((success, fail) -> {
                        if (fail != null) completeExceptionally(fail);
                        else
                        {
                            assert success == null;
                            complete(null);
                        }
                    });
                }
        }
    }
}
