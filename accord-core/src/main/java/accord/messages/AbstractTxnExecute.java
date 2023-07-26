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

import accord.api.Data;
import accord.local.Node;
import accord.local.SafeCommandStore;
import accord.primitives.PartialTxn;
import accord.primitives.Participants;
import accord.primitives.Ranges;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.topology.Topologies;
import accord.utils.async.AsyncChain;

/**
 * Message for executing arbitrary code during a txns ReadyToExecute phase
 */
public abstract class AbstractTxnExecute extends ReadTxnData
{
    public AbstractTxnExecute(Node.Id to, Topologies topologies, TxnId txnId, Participants<?> readScope, Timestamp executeAt)
    {
        super(to, topologies, txnId, readScope, executeAt);
    }

    public AbstractTxnExecute(TxnId txnId, Participants<?> readScope, long executeAtEpoch, long waitForEpoch)
    {
        super(txnId, readScope, executeAtEpoch, waitForEpoch);
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + '{' +
               "txnId:" + txnId +
               '}';
    }

    public MessageType type()
    {
        return null;
    }

    protected abstract AsyncChain<Data> execute(SafeCommandStore safeStore, Timestamp executeAt, PartialTxn txn, Ranges unavailable);
}
