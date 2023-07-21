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

package accord.messages;

import javax.annotation.Nullable;

import accord.api.Read;
import accord.api.UnresolvedData;
import accord.local.Node;
import accord.local.SafeCommandStore;
import accord.primitives.PartialTxn;
import accord.primitives.Participants;
import accord.primitives.Ranges;
import accord.primitives.RoutingKeys;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.topology.Topologies;
import accord.utils.async.AsyncChain;

/**
 * Message for executing arbitrary code during a txns ReadyToExecute phase
 */
public abstract class TxnExecute extends ReadTxnData
{
    public TxnExecute(Node.Id to, Topologies topologies, TxnId txnId, Participants<?> readScope, Timestamp executeAt, @Nullable RoutingKeys dataReadKeys, @Nullable Read followupRead)
    {
        super(to, topologies, txnId, readScope, executeAt, dataReadKeys, followupRead);
    }

    public TxnExecute(TxnId txnId, Participants<?> readScope, long executeAtEpoch, long waitForEpoch, @Nullable RoutingKeys dataReadKeys, @Nullable Read followupRead)
    {
        super(txnId, readScope, executeAtEpoch, waitForEpoch, dataReadKeys, followupRead);
    }

    protected abstract AsyncChain<UnresolvedData> execute(SafeCommandStore safeStore, Timestamp executeAt, PartialTxn txn, Ranges unavailable);
}
