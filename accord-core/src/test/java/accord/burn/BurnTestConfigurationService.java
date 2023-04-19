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

package accord.burn;

import accord.api.MessageSink;
import accord.api.TestableConfigurationService;
import accord.impl.AbstractConfigurationService;
import accord.utils.RandomSource;
import accord.local.Node;
import accord.messages.*;
import accord.topology.Topology;
import accord.utils.async.AsyncResults;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

public class BurnTestConfigurationService extends AbstractConfigurationService implements TestableConfigurationService
{
    private final MessageSink messageSink;
    private final Function<Node.Id, Node> lookup;
    private final Supplier<RandomSource> randomSupplier;
    private final TopologyUpdates topologyUpdates;
    private final Map<Long, FetchTopology> pendingEpochs = new HashMap<>();
    private final Listener postListener;

    public BurnTestConfigurationService(Node.Id node, MessageSink messageSink, Supplier<RandomSource> randomSupplier, Topology topology, Function<Node.Id, Node> lookup, TopologyUpdates topologyUpdates)
    {
        super(node);
        this.messageSink = messageSink;
        this.randomSupplier = randomSupplier;
        this.lookup = lookup;
        this.topologyUpdates = topologyUpdates;
        this.postListener = new Listener()
        {
            @Override
            public void onTopologyUpdate(Topology topology)
            {
                FetchTopology fetch = pendingEpochs.remove(topology.epoch());
                if (fetch == null)
                    return;

                fetch.setSuccess(null);
            }

            @Override
            public void onEpochSyncComplete(Node.Id node, long epoch) {}

            @Override
            public void truncateTopologyUntil(long epoch) {}
        };


        epochs.receive(Topology.EMPTY).acknowledge(0);
        epochs.receive(topology).acknowledge(1);
    }

    private static class FetchTopologyRequest implements Request
    {
        private final long epoch;

        public FetchTopologyRequest(long epoch)
        {
            this.epoch = epoch;
        }

        @Override
        public void process(Node on, Node.Id from, ReplyContext replyContext)
        {
            Topology topology = on.configService().getTopologyForEpoch(epoch);
            on.reply(from, replyContext, new FetchTopologyReply(topology));
        }

        @Override
        public MessageType type()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public String toString()
        {
            return "FetchTopologyRequest{" + epoch + '}';
        }
    }

    private static class FetchTopologyReply implements Reply
    {
        public final Topology topology;

        public FetchTopologyReply(Topology topology)
        {
            this.topology = topology;
        }

        @Override
        public MessageType type()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public String toString()
        {
            String epoch = topology == null ? "null" : Long.toString(topology.epoch());
            return "FetchTopologyReply{" + epoch + '}';
        }
    }

    private class FetchTopology extends AsyncResults.SettableResult<Void> implements Callback<FetchTopologyReply>
    {
        private final FetchTopologyRequest request;
        private final List<Node.Id> candidates;

        public FetchTopology(long epoch)
        {
            this.request = new FetchTopologyRequest(epoch);
            this.candidates = new ArrayList<>();
            sendNext();
        }

        synchronized void sendNext()
        {
            if (candidates.isEmpty())
            {
                candidates.addAll(currentTopology().nodes());
                candidates.remove(node);
            }
            int idx = randomSupplier.get().nextInt(candidates.size());
            Node.Id node = candidates.remove(idx);
            messageSink.send(node, request, this);
        }

        @Override
        public void onSuccess(Node.Id from, FetchTopologyReply reply)
        {
            if (reply.topology != null)
                reportTopology(reply.topology);
            else
                sendNext();
        }

        @Override
        public synchronized void onFailure(Node.Id from, Throwable failure)
        {
            sendNext();
        }

        @Override
        public void onCallbackFailure(Node.Id from, Throwable failure)
        {
            tryFailure(failure);
        }
    }

    @Override
    protected Listener preListener()
    {
        return null;
    }

    @Override
    protected Listener postListener()
    {
        return postListener;
    }

    @Override
    protected void fetchTopologyInternal(long epoch)
    {
        pendingEpochs.computeIfAbsent(epoch, FetchTopology::new);
    }

    @Override
    protected void beginEpochSync(long epoch)
    {
        Topology topology = getTopologyForEpoch(epoch);
        Node originator = lookup.apply(node);
        topologyUpdates.syncEpoch(originator, epoch - 1, topology.nodes());
    }
}
