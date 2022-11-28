package accord.coordinate;

import accord.local.Node;
import accord.local.Node.Id;
import accord.messages.CheckStatus;
import accord.messages.CheckStatus.CheckStatusOk;
import accord.messages.CheckStatus.CheckStatusReply;
import accord.messages.CheckStatus.IncludeInfo;
import accord.primitives.RoutingKeys;
import accord.primitives.TxnId;
import accord.topology.Topologies;

/**
 * A result of null indicates the transaction is globally persistent
 * A result of CheckStatusOk indicates the maximum status found for the transaction, which may be used to assess progress
 */
public abstract class CheckShards extends ReadCoordinator<CheckStatusReply>
{
    final RoutingKeys contactKeys;

    /**
     * The epoch until which we want to fetch data from remotely
     * TODO: configure the epoch we want to start with
     */
    final long untilRemoteEpoch;
    final IncludeInfo includeInfo;

    protected CheckStatusOk merged;

    protected CheckShards(Node node, TxnId txnId, RoutingKeys contactKeys, long srcEpoch, IncludeInfo includeInfo)
    {
        super(node, topologyFor(node, contactKeys, srcEpoch), txnId);
        this.untilRemoteEpoch = srcEpoch;
        this.contactKeys = contactKeys;
        this.includeInfo = includeInfo;
    }

    private static Topologies topologyFor(Node node, RoutingKeys someKeys, long epoch)
    {
        // TODO (now): why were we contacting txnId.epoch...epoch?
        return node.topology().preciseEpochs(someKeys, epoch, epoch);
    }

    @Override
    protected void contact(Id id)
    {
        node.send(id, new CheckStatus(txnId, contactKeys, txnId.epoch, untilRemoteEpoch, includeInfo), this);
    }

    protected boolean isSufficient(Id from, CheckStatusOk ok) { return isSufficient(ok); }
    protected abstract boolean isSufficient(CheckStatusOk ok);

    protected Action checkSufficient(Id from, CheckStatusOk ok)
    {
        if (isSufficient(from, ok))
            return Action.Approve;

        return Action.ApproveIfQuorum;
    }

    @Override
    protected Action process(Id from, CheckStatusReply reply)
    {
        debug.put(from, reply);
        if (reply.isOk())
        {
            CheckStatusOk ok = (CheckStatusOk) reply;
            if (merged == null) merged = ok;
            else merged = merged.merge(ok);

            return checkSufficient(from, ok);
        }
        else
        {
            onFailure(from, new IllegalStateException("Submitted command to a replica that did not own the range"));
            return Action.Abort;
        }
    }
}