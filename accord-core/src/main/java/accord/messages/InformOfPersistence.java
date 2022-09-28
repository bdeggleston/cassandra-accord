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

import java.util.Collections;
import java.util.Set;

import accord.api.Key;
import accord.local.Command;
import accord.local.Node;
import accord.local.Node.Id;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.local.TxnOperation;

import static accord.messages.InformOfTxn.InformOfTxnNack.nack;
import static accord.messages.InformOfTxn.InformOfTxnOk.ok;

public class InformOfPersistence implements Request, TxnOperation
{
    public final TxnId txnId;
    public final Key homeKey;
    public final Timestamp executeAt;
    public final Set<Id> persistedOn;

    public InformOfPersistence(TxnId txnId, Key homeKey, Timestamp executeAt, Set<Id> persistedOn)
    {
        this.txnId = txnId;
        this.homeKey = homeKey;
        this.executeAt = executeAt;
        this.persistedOn = persistedOn;
    }

    public void process(Node node, Id replyToNode, ReplyContext replyContext)
    {
        Reply reply = node.ifLocal(this, homeKey, txnId, instance -> {
            Command command = instance.command(txnId);
            command.setGloballyPersistent(homeKey, executeAt);
            instance.progressLog().executedOnAllShards(command, persistedOn);
            return ok();
        });

        if (reply == null)
            reply = nack();

        node.reply(replyToNode, replyContext, reply);
    }

    @Override
    public Iterable<TxnId> txnIds()
    {
        return Collections.singleton(txnId);
    }

    @Override
    public Iterable<Key> keys()
    {
        return Collections.emptyList();
    }

    @Override
    public String toString()
    {
        return "InformOfPersistence{" +
               "txnId:" + txnId +
               '}';
    }

    @Override
    public MessageType type()
    {
        return MessageType.INFORM_PERSISTED_REQ;
    }
}
