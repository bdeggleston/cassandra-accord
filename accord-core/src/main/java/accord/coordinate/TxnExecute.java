/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package accord.coordinate;

import accord.api.Data;
import accord.api.Result;
import accord.local.Node;
import accord.local.Node.Id;
import accord.messages.Commit;
import accord.messages.ReadData.ReadNack;
import accord.messages.ReadData.ReadOk;
import accord.messages.ReadData.ReadReply;
import accord.messages.ReadTxnData;
import accord.primitives.*;
import accord.primitives.Txn.Kind;
import accord.topology.Topologies;
import accord.topology.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static accord.coordinate.ReadCoordinator.Action.*;
import static accord.messages.Commit.Kind.Maximal;
import static accord.utils.Invariants.checkArgument;

public class TxnExecute extends ReadCoordinator<ReadReply> implements Execute
{
    public static final Execute.Factory FACTORY = TxnExecute::new;

    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(TxnExecute.class);

    final Txn txn;
    final Participants<?> readScope;
    final FullRoute<?> route;
    final Timestamp executeAt;
    final Deps deps;
    final Topologies executes;
    final BiConsumer<? super Result, Throwable> callback;
    private Data data;

    private TxnExecute(Node node, TxnId txnId, Txn txn, FullRoute<?> route, Participants<?> readScope, Timestamp executeAt, Deps deps, BiConsumer<? super Result, Throwable> callback)
    {
        super(node, node.topology().forEpoch(readScope, executeAt.epoch()), txnId, txn.readDataCL());
        this.txn = txn;
        this.route = route;
        this.readScope = readScope;
        this.executeAt = executeAt;
        this.deps = deps;
        this.executes = node.topology().forEpoch(route, executeAt.epoch());
        this.callback = callback;
    }

    public static void execute(Node node, TxnId txnId, Txn txn, FullRoute<?> route, Timestamp executeAt, Deps deps, BiConsumer<? super Result, Throwable> callback)
    {
        Seekables<?, ?> readKeys = txn.read().keys();
        Participants<?> readScope = readKeys.toParticipants();
        // Recovery calls execute and we would like execute to run BlockOnDeps because that will notify the agent
        // of the local barrier
        // TODO we don't really need to run BlockOnDeps, executing the empty txn would also be fine
        if (txn.kind() == Kind.SyncPoint)
        {
            checkArgument(txnId.equals(executeAt));
            BlockOnDeps.blockOnDeps(node, txnId, txn, route, deps, callback);
        }
        else if (readKeys.isEmpty())
        {
            Result result = txn.result(txnId, executeAt, null);
            Consumer<Throwable> onAppliedToQuorum = null;
            DataConsistencyLevel writeDataCL = txn.writeDataCL();
            if (!writeDataCL.requiresSynchronousCommit)
                callback.accept(result, null);
            else
                onAppliedToQuorum = (applyFailure) -> callback.accept(applyFailure == null ? result : null, applyFailure);
            Persist.persist(node, txnId, route, txn, executeAt, deps, txn.execute(txnId, executeAt, null), result, onAppliedToQuorum);
        }
        else
        {
            TxnExecute execute = new TxnExecute(node, txnId, txn, route, readScope, executeAt, deps, callback);
            execute.start(readScope);
        }
    }

    @Override
    protected void sendInitialReads(Map<Id, RoutingKeys> readSet)
    {
        Commit.commitMinimalAndRead(node, executes, txnId, txn, route, readScope, executeAt, deps, readSet, this);
    }

    @Override
    public void contact(Id to)
    {
        node.send(to, new ReadTxnData(to, topologies(), txnId, readScope, executeAt, null, null), this);
    }

    @Override
    protected Ranges unavailable(ReadReply reply)
    {
        return ((ReadOk)reply).unavailable;
    }

    @Override
    protected Action process(Id from, ReadReply reply)
    {
        if (reply.isOk())
        {
            ReadOk ok = ((ReadOk) reply);
            Data next = ((ReadOk) reply).data;
            if (next != null)
                data = data == null ? next : data.merge(next);

            if (ok.unavailable == null)
            {
                return txn.readDataCL().requiresDigestReads ? ApproveIfQuorum : Approve;
            }
            // TODO partial interaction with quorum
            return ApprovePartial;
        }

        ReadNack nack = (ReadNack) reply;
        switch (nack)
        {
            default: throw new IllegalStateException();
            case Error:
                // TODO (expected): report content of error
                return Action.Reject;
            case Redundant:
                callback.accept(null, new Preempted(txnId, route.homeKey()));
                return Action.Aborted;
            case NotCommitted:
                // the replica may be missing the original commit, or the additional commit, so send everything
                Topologies topology = node.topology().preciseEpochs(route, txnId.epoch(), executeAt.epoch());
                Topology coordinateTopology = topology.forEpoch(txnId.epoch());
                node.send(from, new Commit(Maximal, from, coordinateTopology, topology, txnId, txn, route, readScope, executeAt, deps, null));
                // also try sending a read command to another replica, in case they're ready to serve a response
                return Action.TryAlternative;
            case Invalid:
                callback.accept(null, new IllegalStateException("Submitted a read command to a replica that did not own the range"));
                return Action.Aborted;
        }
    }

    @Override
    protected void onDone(Success success, Throwable failure)
    {
        if (failure == null)
        {
            Result result = txn.result(txnId, executeAt, data);

            // If this transaction generates repair writes then we don't want to acknowledge it until the writes are committed
            // to make sure the transaction's reads are monotonic from the perspective of the caller
            // If the transaction specified a writeDataCL then we don't want to acknowledge until the CL is met
            Consumer<Throwable> onAppliedToQuorum = null;
            if (txn.writeDataCL().requiresSynchronousCommit)
                onAppliedToQuorum = (applyFailure) -> callback.accept(applyFailure == null ? result : null, applyFailure);
            else
                callback.accept(result, null);            // avoid re-calculating topologies if it is unchanged
            Persist.persist(node, executes, txnId, route, txn, executeAt, deps, txn.execute(txnId, executeAt, data), result, onAppliedToQuorum);
        }
        else
        {
            callback.accept(null, failure);
        }
    }
}
