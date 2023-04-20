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
import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.AbstractIterator;
import com.google.common.primitives.Ints;
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

    protected final EpochHistory epochs = new EpochHistory();
    private static class Listeners implements Iterable<Listener>
    {
        private static class Iter extends AbstractIterator<Listener>
        {
            private enum State {PRE, REGISTERED, POST, COMPLETE}
            private State state = State.PRE;
            private final Listener preListener;
            private final Iterator<Listener> registered;
            private final Listener postListener;

            public Iter(Listener preListener, Iterator<Listener> registered, Listener postListener)
            {
                this.preListener = preListener;
                this.registered = registered;
                this.postListener = postListener;
            }

            @Override
            protected Listener computeNext()
            {
                switch (state)
                {
                    case PRE:
                        state = State.REGISTERED;
                        if (preListener != null)
                            return preListener;
                    case REGISTERED:
                        if (registered.hasNext())
                            return registered.next();
                        state = State.POST;
                    case POST:
                        state = State.COMPLETE;
                        if (postListener != null)
                            return postListener;
                    default:
                        throw new IllegalStateException();
                    case COMPLETE:
                        return endOfData();
                }
            }
        }

        private final Supplier<Listener> preListener;
        private final Supplier<Listener> postListener;
        private final List<Listener> registered = new ArrayList<>();

        public Listeners(Supplier<Listener> preListener, Supplier<Listener> postListener)
        {
            this.preListener = preListener;
            this.postListener = postListener;
        }

        public void add(Listener listener)
        {
            registered.add(listener);
        }

        @Override
        public Iterator<Listener> iterator()
        {
            return new Iter(preListener.get(), registered.iterator(), postListener.get());
        }
    }

    protected final Listeners listeners = new Listeners(this::preListener, this::postListener);

    static class EpochState
    {
        private final long epoch;
        private final AsyncResult.Settable<Topology> received = AsyncResults.settable();
        private final AsyncResult.Settable<Void> acknowledged = AsyncResults.settable();

        private Topology topology = null;

        public EpochState(long epoch)
        {
            this.epoch = epoch;
        }

        public long epoch()
        {
            return epoch;
        }

        @Override
        public String toString()
        {
            return "EpochState{" + epoch + '}';
        }
    }

    @VisibleForTesting
    protected static class EpochHistory
    {
        // TODO (low priority): move pendingEpochs / FetchTopology into here?
        private List<EpochState> epochs = new ArrayList<>();

        private long lastReceived = 0;
        private long lastAcknowledged = 0;

        long minEpoch()
        {
            return epochs.isEmpty() ? 0L : epochs.get(0).epoch;
        }

        long maxEpoch()
        {
            int size = epochs.size();
            return size == 0 ? 0L : epochs.get(size - 1).epoch;
        }

        @VisibleForTesting
        EpochState atIndex(int idx)
        {
            return epochs.get(idx);
        }

        @VisibleForTesting
        int size()
        {
            return epochs.size();
        }

        EpochState getOrCreate(long epoch)
        {
            Invariants.checkArgument(epoch > 0);
            if (epochs.isEmpty())
            {
                EpochState state = new EpochState(epoch);
                epochs.add(state);
                return state;
            }

            long minEpoch = minEpoch();
            if (epoch < minEpoch)
            {
                int prepend = Ints.checkedCast(minEpoch - epoch);
                List<EpochState> next = new ArrayList<>(epochs.size() + prepend);
                for (long addEpoch=epoch; addEpoch<minEpoch; addEpoch++)
                    next.add(new EpochState(addEpoch));
                next.addAll(epochs);
                epochs = next;
                minEpoch = minEpoch();
                Invariants.checkState(minEpoch == epoch);
            }
            long maxEpoch = maxEpoch();
            int idx = Ints.checkedCast(epoch - minEpoch);

            // add any missing epochs
            for (long addEpoch = maxEpoch + 1; addEpoch <= epoch; addEpoch++)
                epochs.add(new EpochState(addEpoch));

            return epochs.get(idx);
        }

        public EpochHistory receive(Topology topology)
        {
            long epoch = topology.epoch();
            Invariants.checkState(epoch == 0 || lastReceived == epoch - 1);
            lastReceived = epoch;
            EpochState state = getOrCreate(epoch);
            if (state != null)
            {
                state.topology = topology;
                state.received.setSuccess(topology);
            }
            return this;
        }

        AsyncResult<Topology> receiveFuture(long epoch)
        {
            return getOrCreate(epoch).received;
        }

        Topology topologyFor(long epoch)
        {
            return getOrCreate(epoch).topology;
        }

        public EpochHistory acknowledge(long epoch)
        {
            Invariants.checkState(epoch == 0 || lastAcknowledged == epoch - 1);
            lastAcknowledged = epoch;
            getOrCreate(epoch).acknowledged.setSuccess(null);
            return this;
        }

        AsyncResult<Void> acknowledgeFuture(long epoch)
        {
            return getOrCreate(epoch).acknowledged;
        }

        void truncateUntil(long epoch)
        {
            Invariants.checkArgument(epoch < maxEpoch());
            long minEpoch = minEpoch();
            int toTrim = Ints.checkedCast(epoch - minEpoch);
            if (toTrim <=0)
                return;

            epochs = new ArrayList<>(epochs.subList(toTrim, epochs.size()));
        }
    }

    public AbstractConfigurationService(Node.Id node)
    {
        this.node = node;
    }

    /**
     * Return a listener to be called before any normally registered listeners
     */
    protected abstract Listener preListener();
    /**
     * Return a listener to be called after any normally registered listeners
     */
    protected abstract Listener postListener();

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

        for (Listener listener : listeners)
            listener.onTopologyUpdate(topology);
        epochs.receive(topology);
    }

    public synchronized void epochSyncComplete(Node.Id node, long epoch)
    {
        for (Listener listener : listeners)
            listener.onEpochSyncComplete(node, epoch);
    }

    public synchronized void truncateTopologiesUntil(long epoch)
    {
        for (Listener listener : listeners)
            listener.truncateTopologyUntil(epoch);
        epochs.truncateUntil(epoch);
    }
}
