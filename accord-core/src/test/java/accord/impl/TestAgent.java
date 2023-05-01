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

package accord.impl;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.Agent;
import accord.api.Result;
import accord.impl.mock.MockStore;
import accord.local.Command;
import accord.local.Node;
import accord.primitives.Keys;
import accord.primitives.Ranges;
import accord.primitives.Seekables;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;

public class TestAgent implements Agent
{
    private static final Logger logger = LoggerFactory.getLogger(TestAgent.class);

    public static final ConcurrentMap<Timestamp, AtomicInteger> completedLocalBarriers = new ConcurrentHashMap<>();

    @Override
    public void onRecover(Node node, Result success, Throwable fail)
    {
        // do nothing, intended for use by implementations to decide what to do about recovered transactions
        // specifically if and how they should inform clients of the result
        // e.g. in Maelstrom we send the full result directly, in other impls we may simply acknowledge success via the coordinator
    }

    @Override
    public void onInconsistentTimestamp(Command command, Timestamp prev, Timestamp next)
    {
        throw new AssertionError();
    }

    @Override
    public void onFailedBootstrap(String phase, Ranges ranges, Runnable retry, Throwable failure)
    {
        retry.run();
    }

    @Override
    public void onUncaughtException(Throwable t)
    {
        logger.error("Uncaught exception", t);
    }

    @Override
    public void onHandledException(Throwable t)
    {
    }

    @Override
    public boolean isExpired(TxnId initiated, long now)
    {
        return TimeUnit.SECONDS.convert(now - initiated.hlc(), TimeUnit.MICROSECONDS) >= 10;
    }

    @Override
    public Txn emptyTxn(Txn.Kind kind, Seekables<?, ?> keysOrRanges)
    {
        return new Txn.InMemory(kind, keysOrRanges, MockStore.read(Keys.EMPTY), MockStore.RESOLVER, MockStore.QUERY, null);
    }

    @Override
    public void onLocalBarrier(@Nonnull Seekables<?, ?> keysOrRanges, @Nonnull Timestamp executeAt) {
        completedLocalBarriers.computeIfAbsent(executeAt, ignored -> new AtomicInteger()).incrementAndGet();
    }

}
