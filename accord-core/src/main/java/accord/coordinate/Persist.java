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

import java.util.HashSet;
import java.util.Set;
import java.util.function.Consumer;

import accord.api.Result;
import accord.coordinate.tracking.QuorumTracker;
import accord.coordinate.tracking.RequestStatus;
import accord.local.Node;
import accord.local.Node.Id;
import accord.messages.Apply;
import accord.messages.Apply.ApplyReply;
import accord.messages.Callback;
import accord.messages.Commit;
import accord.messages.InformDurable;
import accord.primitives.DataConsistencyLevel;
import accord.primitives.Deps;
import accord.primitives.FullRoute;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.primitives.Writes;
import accord.topology.Topologies;
import javax.annotation.Nullable;

import static accord.coordinate.tracking.RequestStatus.Success;
import static accord.local.Status.Durability.Majority;
import static accord.messages.Apply.executes;
import static accord.messages.Apply.participates;
import static accord.messages.Commit.Kind.Maximal;

public class Persist implements Callback<ApplyReply>
{
    final Node node;
    final TxnId txnId;
    final FullRoute<?> route;
    final Txn txn;
    final Timestamp executeAt;
    final Deps deps;
    final QuorumTracker tracker;
    final Set<Id> persistedOn;
    // Quorum is the only CL supported right now
    // Deferring handle other consistency levels to LOCAL_* support
    @Nullable
    final Consumer<Throwable> onAppliedToQuorum;
    boolean isDone;
    Throwable fail;

    public static void persist(Node node, TxnId txnId, FullRoute<?> route, Txn txn, Timestamp executeAt, Deps deps, Writes writes, Result result, @Nullable Consumer<Throwable> onAppliedToQuorum)
    {
        Topologies executes = executes(node, route, executeAt);
        persist(node, executes, txnId, route, txn, executeAt, deps, writes, result, onAppliedToQuorum);
    }

    public static void persist(Node node, Topologies executes, TxnId txnId, FullRoute<?> route, Txn txn, Timestamp executeAt, Deps deps, Writes writes, Result result, @Nullable Consumer<Throwable> onAppliedToQuorum)
    {
        Topologies participates = participates(node, route, txnId, executeAt, executes);
        Persist persist = new Persist(node, executes, txnId, route, txn, executeAt, deps, onAppliedToQuorum, writes.writeDataCL);
        node.send(participates.nodes(), to -> Apply.applyMinimal(to, participates, executes, txnId, route, txn, executeAt, deps, writes, result), persist);
    }

    public static void persistMaximal(Node node, TxnId txnId, FullRoute<?> route, Txn txn, Timestamp executeAt, Deps deps, Writes writes, Result result)
    {
        Topologies executes = executes(node, route, executeAt);
        Topologies participates = participates(node, route, txnId, executeAt, executes);
        //TODO (review) I think write data CL doesn't matter for recovery since there is no callback?
        Persist persist = new Persist(node, participates, txnId, route, txn, executeAt, deps, null, DataConsistencyLevel.UNSPECIFIED);
        node.send(participates.nodes(), to -> Apply.applyMaximal(to, participates, executes, txnId, route, txn, executeAt, deps, writes, result), persist);
    }

    private Persist(Node node, Topologies topologies, TxnId txnId, FullRoute<?> route, Txn txn, Timestamp executeAt, Deps deps, @Nullable Consumer<Throwable> onAppliedToQuorum, DataConsistencyLevel writeDataCL)
    {
        this.node = node;
        this.txnId = txnId;
        this.txn = txn;
        this.deps = deps;
        this.route = route;
        this.tracker = new QuorumTracker(topologies, writeDataCL);
        this.executeAt = executeAt;
        this.persistedOn = new HashSet<>();
        this.onAppliedToQuorum = onAppliedToQuorum;
    }

    @Override
    public void onSuccess(Id from, ApplyReply reply)
    {
        switch (reply)
        {
            default: throw new IllegalStateException();
            case Redundant:
            case Applied:
                persistedOn.add(from);
                if (tracker.recordSuccess(from) == Success)
                {
                    if (!isDone)
                    {
                        if (onAppliedToQuorum != null)
                            onAppliedToQuorum.accept(null);
                        isDone = true;
                        Topologies topologies = tracker.topologies();
                        node.send(topologies.nodes(), to -> new InformDurable(to, topologies, route, txnId, executeAt, Majority));
                    }
                }
                break;
            case Insufficient:
                Topologies topologies = node.topology().preciseEpochs(route, txnId.epoch(), executeAt.epoch());
                // TODO (easy, cleanup): use static method in Commit
                node.send(from, new Commit(Maximal, from, topologies.forEpoch(txnId.epoch()), topologies, txnId, txn, route, null, executeAt, deps));
        }
    }

    @Override
    public void onFailure(Id from, Throwable failure)
    {
        if (fail == null)
            fail = failure;
        else
            fail.addSuppressed(failure);

        // Now that the transaction result callback may not be invoked until Persist it's important to invoke the callback on failure
        // so Node can clean up the transaction state
        RequestStatus status = tracker.recordFailure(from);
        if (!isDone && status == RequestStatus.Failed)
        {
            isDone = true;
            if (onAppliedToQuorum != null)
                onAppliedToQuorum.accept(fail);
        }
        // TODO (desired, consider): send knowledge of partial persistence?
    }

    @Override
    public void onCallbackFailure(Id from, Throwable failure)
    {
    }
}
