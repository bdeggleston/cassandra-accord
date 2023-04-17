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

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.ConfigurationService;
import accord.local.Node;
import accord.topology.Topology;
import accord.utils.Invariants;
import accord.utils.async.AsyncResult;
import accord.utils.async.AsyncResults;

public abstract class AbstractConfigurationService implements ConfigurationService
{
    private static final Logger logger = LoggerFactory.getLogger(AbstractConfigurationService.class);

    protected final Node.Id node;

    private final EpochHistory epochs = new EpochHistory();
    private final List<Listener> listeners = new ArrayList<>();

    private static class EpochState
    {
        private final long epoch;
        private final AsyncResult.Settable<Topology> received = AsyncResults.settable();
        private final AsyncResult.Settable<Void> acknowledged = AsyncResults.settable();
        private final AsyncResult.Settable<Void> synced = AsyncResults.settable();

        private Topology topology = null;

        public EpochState(long epoch)
        {
            this.epoch = epoch;
        }
    }

    private static class EpochHistory
    {
        // TODO (low priority): move pendingEpochs / FetchTopology into here?
        private final List<EpochState> epochs = new ArrayList<>();

        private long lastReceived = 0;
        private long lastAcknowledged = 0;
        private long lastSyncd = 0;

        private EpochState get(long epoch)
        {
            for (long addEpoch = epochs.size() - 1; addEpoch <= epoch; addEpoch++)
                epochs.add(new EpochState(addEpoch));
            return epochs.get((int) epoch);
        }

        EpochHistory receive(Topology topology)
        {
            long epoch = topology.epoch();
            Invariants.checkState(epoch == 0 || lastReceived == epoch - 1);
            lastReceived = epoch;
            EpochState state = get(epoch);
            state.topology = topology;
            state.received.setSuccess(topology);
            return this;
        }

        AsyncResult<Topology> receiveFuture(long epoch)
        {
            return get(epoch).received;
        }

        Topology topologyFor(long epoch)
        {
            return get(epoch).topology;
        }

        EpochHistory acknowledge(long epoch)
        {
            Invariants.checkState(epoch == 0 || lastAcknowledged == epoch - 1);
            lastAcknowledged = epoch;
            get(epoch).acknowledged.setSuccess(null);
            return this;
        }

        AsyncResult<Void> acknowledgeFuture(long epoch)
        {
            return get(epoch).acknowledged;
        }

        EpochHistory syncComplete(long epoch)
        {
            Invariants.checkState(epoch == 0 || lastSyncd == epoch - 1);
            EpochState state = get(epoch);
            Invariants.checkState(state.received.isDone());
            Invariants.checkState(state.acknowledged.isDone());
            lastSyncd = epoch;
            get(epoch).synced.setSuccess(null);
            return this;
        }
    }

    public AbstractConfigurationService(Node.Id node)
    {
        this.node = node;
    }

    protected void loadTopologySyncComplete(Topology topology)
    {
        long epoch = topology.epoch();
        epochs.receive(topology).acknowledge(epoch).syncComplete(epoch);
    }

    @Override
    public synchronized void registerListener(Listener listener)
    {
        listeners.add(listener);
    }

    @Override
    public synchronized Topology currentTopology()
    {
        return epochs.topologyFor(epochs.lastReceived);
    }

    @Override
    public synchronized Topology getTopologyForEpoch(long epoch)
    {
        return epochs.topologyFor(epoch);
    }

    protected abstract void fetchTopologyInternal(long epoch);

    @Override
    public synchronized void fetchTopologyForEpoch(long epoch)
    {
        if (epoch <= epochs.lastReceived)
            return;

        fetchTopologyInternal(epoch);
    }

    protected abstract void beginEpochSync(long epoch);

    @Override
    public synchronized void acknowledgeEpoch(long epoch)
    {
        epochs.acknowledge(epoch);
        beginEpochSync(epoch);
    }

    protected abstract void topologyReportedPreListenerUpdate(Topology topology);
    protected abstract void topologyReportedPostListenerUpdate(Topology topology);

    public synchronized void reportTopology(Topology topology)
    {
        long lastReceived = epochs.lastReceived;
        if (topology.epoch() <= lastReceived)
            return;

        if (topology.epoch() > lastReceived + 1)
        {
            fetchTopologyForEpoch(lastReceived + 1);
            epochs.receiveFuture(lastReceived + 1).addCallback(() -> reportTopology(topology));
            return;
        }

        long lastAcked = epochs.lastAcknowledged;
        if (topology.epoch() > lastAcked + 1)
        {
            epochs.acknowledgeFuture(lastAcked + 1).addCallback(() -> reportTopology(topology));
            return;
        }
        logger.trace("Epoch {} received by {}", topology.epoch(), node);

        topologyReportedPreListenerUpdate(topology);
        epochs.receive(topology);
        for (Listener listener : listeners)
            listener.onTopologyUpdate(topology);

        topologyReportedPostListenerUpdate(topology);
    }

    protected abstract void epochSyncCompletePreListenerUpdate(Node.Id node, long epoch);
    protected abstract void epochSyncCompletePostListenerUpdate(Node.Id node, long epoch);

    public synchronized void epochSyncComplete(Node.Id node, long epoch)
    {
        epochSyncCompletePreListenerUpdate(node, epoch);
        for (Listener listener : listeners)
            listener.onEpochSyncComplete(node, epoch);
        epochSyncCompletePostListenerUpdate(node, epoch);
    }
}
