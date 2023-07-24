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

import java.util.function.BiConsumer;
import java.util.function.Consumer;

import accord.api.Result;
import accord.local.Node;
import accord.primitives.DataConsistencyLevel;
import accord.primitives.Deps;
import accord.primitives.FullRoute;
import accord.primitives.Participants;
import accord.primitives.Seekables;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.primitives.Unseekables;

import static accord.utils.Invariants.checkArgument;

public interface Execute
{
    interface Factory
    {
        Execute create(Node node, TxnId txnId, Txn txn, FullRoute<?> route, Participants<?> readScope, Timestamp executeAt, Deps deps, BiConsumer<? super Result, Throwable> callback);
    }

    void start(Unseekables<?> scope);

    static void execute(Node node, TxnId txnId, Txn txn, FullRoute<?> route, Timestamp executeAt, Deps deps, BiConsumer<? super Result, Throwable> callback)
    {
        Seekables<?, ?> readKeys = txn.read().keys();
        Participants<?> readScope = readKeys.toParticipants();
        // Recovery calls execute and we would like execute to run BlockOnDeps because that will notify the agent
        // of the local barrier
        // TODO we don't really need to run BlockOnDeps, executing the empty txn would also be fine
        if (txn.kind() == Txn.Kind.SyncPoint)
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
            Execute execute = node.executionFactory().create(node, txnId, txn, route, readScope, executeAt, deps, callback);
            execute.start(readScope);
        }
    }
}
