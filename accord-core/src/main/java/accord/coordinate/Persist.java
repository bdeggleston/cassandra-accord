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

import accord.api.Result;
import accord.coordinate.tracking.QuorumTracker;
import accord.local.Node;
import accord.local.Node.Id;
import accord.messages.Apply;
import accord.messages.Apply.ApplyReply;
import accord.messages.Callback;
import accord.messages.Commit;
import accord.messages.InformDurable;
import accord.primitives.*;
import accord.topology.Topologies;

import java.util.HashSet;
import java.util.Set;
import java.util.function.BiConsumer;

import static accord.coordinate.tracking.RequestStatus.Success;
import static accord.local.Status.Durability.Majority;
import static accord.messages.Apply.executes;
import static accord.messages.Apply.participates;
import static accord.messages.Commit.Kind.Maximal;

public abstract class Persist implements Callback<ApplyReply>
{
    public interface Factory
    {
        Persist create(Node node, Topologies topologies, TxnId txnId, FullRoute<?> route, Txn txn, Timestamp executeAt, Deps deps);
    }

    final Node node;
    final TxnId txnId;
    final FullRoute<?> route;
    final Txn txn;
    final Timestamp executeAt;
    final Deps deps;
    final QuorumTracker tracker;
    final Set<Id> persistedOn;
    boolean isDone;

    public static void persist(Node node, TxnId txnId, FullRoute<?> route, Txn txn, Timestamp executeAt, Deps deps, Writes writes, Result result, BiConsumer<? super Result, Throwable> clientCallback)
    {
        Topologies executes = executes(node, route, executeAt);
        persist(node, executes, txnId, route, txn, executeAt, deps, writes, result, clientCallback);
    }

    public static void persist(Node node, Topologies executes, TxnId txnId, FullRoute<?> route, Txn txn, Timestamp executeAt, Deps deps, Writes writes, Result result, BiConsumer<? super Result, Throwable> clientCallback)
    {
        Topologies participates = participates(node, route, txnId, executeAt, executes);
        node.persistFactory().create(node, executes, txnId, route, txn, executeAt, deps)
                             .applyMinimal(participates, executes, writes, result, clientCallback);
    }

    public static void persistMaximal(Node node, TxnId txnId, FullRoute<?> route, Txn txn, Timestamp executeAt, Deps deps, Writes writes, Result result)
    {
        Topologies executes = executes(node, route, executeAt);
        Topologies participates = participates(node, route, txnId, executeAt, executes);
        //TODO (review) I think write data CL doesn't matter for recovery since there is no callback?
        node.persistFactory().create(node, executes, txnId, route, txn, executeAt, deps)
                             .applyMaximal(participates, executes, writes, result, null);
    }

    protected Persist(Node node, Topologies topologies, TxnId txnId, FullRoute<?> route, Txn txn, Timestamp executeAt, Deps deps)
    {
        this.node = node;
        this.txnId = txnId;
        this.txn = txn;
        this.deps = deps;
        this.route = route;
        this.tracker = new QuorumTracker(topologies);
        this.executeAt = executeAt;
        this.persistedOn = new HashSet<>();
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
        // TODO (desired, consider): send knowledge of partial persistence?
    }

    @Override
    public void onCallbackFailure(Id from, Throwable failure)
    {
    }

    public void applyMinimal(Topologies participates, Topologies executes, Writes writes, Result result, BiConsumer<? super Result, Throwable> clientCallback)
    {
        node.send(participates.nodes(), to -> Apply.applyMinimal(to, participates, executes, txnId, route, txn, executeAt, deps, writes, result), this);
        notifyClient(result, clientCallback);
    }
    public void applyMaximal(Topologies participates, Topologies executes, Writes writes, Result result, BiConsumer<? super Result, Throwable> clientCallback)
    {
        node.send(participates.nodes(), to -> Apply.applyMaximal(to, participates, executes, txnId, route, txn, executeAt, deps, writes, result), this);
        notifyClient(result, clientCallback);
    }

    public abstract void notifyClient(Result result, BiConsumer<? super Result, Throwable> clientCallback);
}
