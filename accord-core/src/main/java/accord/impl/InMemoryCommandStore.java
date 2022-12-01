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

import accord.api.Agent;
import accord.api.DataStore;
import accord.api.Key;
import accord.api.ProgressLog;
import accord.api.RoutingKey;
import accord.local.*;
import accord.local.CommandsForKey;
import accord.local.Node;
import accord.local.NodeTimeService;
import accord.local.PreLoadContext;
import accord.local.SafeCommandStore;
import accord.local.SafeCommandStores.ContextState;
import accord.primitives.*;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

public abstract class InMemoryCommandStore extends CommandStore implements CommandsForKey.CommandLoader<TxnId>
{
    private static final Logger logger = LoggerFactory.getLogger(InMemoryCommandStore.class);

    public static class InMemoryState
    {
        private final NodeTimeService time;
        private final Agent agent;
        private final DataStore store;
        private final ProgressLog progressLog;
        private final RangesForEpoch rangesForEpoch;

        private final InMemoryCommandStore commandStore;
        private final NavigableMap<TxnId, Command> commands = new TreeMap<>();
        private final NavigableMap<RoutingKey, CommandsForKey> commandsForKey = new TreeMap<>();

        public InMemoryState(NodeTimeService time, Agent agent, DataStore store, ProgressLog progressLog, RangesForEpoch rangesForEpoch, InMemoryCommandStore commandStore)
        {
            this.time = time;
            this.agent = agent;
            this.store = store;
            this.progressLog = progressLog;
            this.rangesForEpoch = rangesForEpoch;
            this.commandStore = commandStore;
        }

        public Command command(TxnId txnId)
        {
            return commands.get(txnId);
        }

        public boolean hasCommand(TxnId txnId)
        {
            return commands.containsKey(txnId);
        }

        public CommandsForKey commandsForKey(Key key)
        {
            return commandsForKey.get(key);
        }

        public boolean hasCommandsForKey(Key key)
        {
            return commandsForKey.containsKey(key);
        }


        public void forWitnessed(CommandsForKey cfk, Timestamp minTs, Timestamp maxTs, Consumer<Command> consumer)
        {
            cfk.uncommitted().between(minTs, maxTs)
                    .map(txnIdWithExecuteAt -> command(txnIdWithExecuteAt.txnId()))
                    .filter(cmd -> cmd.hasBeen(Status.PreAccepted)).forEach(consumer);
            cfk.committedById().between(minTs, maxTs).map(this::command).forEach(consumer);
            cfk.committedByExecuteAt().between(minTs, maxTs)
                    .map(this::command)
                    .filter(cmd -> cmd.txnId().compareTo(minTs) < 0 || cmd.txnId().compareTo(maxTs) > 0).forEach(consumer);
        }

        public void forEpochCommands(KeyRanges ranges, long epoch, Consumer<Command> consumer)
        {
            Timestamp minTimestamp = new Timestamp(epoch, Long.MIN_VALUE, Integer.MIN_VALUE, Node.Id.NONE);
            Timestamp maxTimestamp = new Timestamp(epoch, Long.MAX_VALUE, Integer.MAX_VALUE, Node.Id.MAX);
            for (KeyRange range : ranges)
            {
                Iterable<CommandsForKey> rangeCommands = commandsForKey.subMap(range.start(),
                                                                               range.startInclusive(),
                                                                               range.end(),
                                                                               range.endInclusive()).values();
                for (CommandsForKey commands : rangeCommands)
                {
                    forWitnessed(commands, minTimestamp, maxTimestamp, cmd -> consumer.accept(cmd));
                }
            }
        }

        public void forCommittedInEpoch(KeyRanges ranges, long epoch, Consumer<Command> consumer)
        {
            Timestamp minTimestamp = new Timestamp(epoch, Long.MIN_VALUE, Integer.MIN_VALUE, Node.Id.NONE);
            Timestamp maxTimestamp = new Timestamp(epoch, Long.MAX_VALUE, Integer.MAX_VALUE, Node.Id.MAX);
            for (KeyRange range : ranges)
            {
                Iterable<CommandsForKey> rangeCommands = commandsForKey.subMap(range.start(),
                                                                                       range.startInclusive(),
                                                                                       range.end(),
                                                                                       range.endInclusive()).values();
                for (CommandsForKey commands : rangeCommands)
                {

                    Collection<Command> committed = commands.committedByExecuteAt()
                            .between(minTimestamp, maxTimestamp).map(this::command).collect(Collectors.toList());
                    committed.forEach(consumer);
                }
            }
        }
    }

    private static class InMemorySafeStore implements SafeCommandStore
    {
        private final InMemoryState state;
        private final PreLoadContext preLoadContext;

        private final ContextState<TxnId, Command, Command.Update> commands;
        private final ContextState<RoutingKey, CommandsForKey, CommandsForKey.Update> commandsForKey;

        public InMemorySafeStore(InMemoryState state, PreLoadContext context)
        {
            this.state = state;
            this.preLoadContext = context;
            this.commands = new ContextState<>(context.txnIds(), state.commands::get, Command.NotWitnessed::create);
            this.commandsForKey = new ContextState<>(context.keys(), state.commandsForKey::get, rk -> new CommandsForKey((Key) rk, commandStore()));
        }

        @Override
        public Command ifPresent(TxnId txnId)
        {
            return commands.get(txnId);
        }

        @Override
        public Command ifLoaded(TxnId txnId)
        {
            return commands.get(txnId);
        }

        @Override
        public Command command(TxnId txnId)
        {
            Command command = commands.get(txnId);
            if (command == null)
                throw new IllegalStateException(String.format("%s was not specified in PreLoadContext", txnId));
            return command;
        }

        @Override
        public CommandsForKey commandsForKey(Key key)
        {
            CommandsForKey cfk = commandsForKey.get(key);
            if (cfk == null)
                throw new IllegalStateException(String.format("%s was not specified in PreLoadContext", key));
            return cfk;
        }

        @Override
        public CommandsForKey maybeCommandsForKey(Key key)
        {
            return commandsForKey.get(key);
        }

        @Override
        public boolean canExecuteWith(PreLoadContext context)
        {
            return context.isSubsetOf(preLoadContext);
        }

        @Override
        public InMemoryCommandStore commandStore()
        {
            return state.commandStore;
        }

        @Override
        public DataStore dataStore()
        {
            return state.store;
        }

        @Override
        public Agent agent()
        {
            return state.agent;
        }

        @Override
        public ProgressLog progressLog()
        {
            return state.progressLog;
        }

        @Override
        public RangesForEpoch ranges()
        {
            return state.rangesForEpoch;
        }

        @Override
        public long latestEpoch()
        {
            return state.time.epoch();
        }

        @Override
        public Timestamp preaccept(TxnId txnId, Keys keys)
        {
            Timestamp max = maxConflict(keys);
            long epoch = latestEpoch();
            if (txnId.compareTo(max) > 0 && txnId.epoch >= epoch && !state.agent.isExpired(txnId, state.time.now()))
                return txnId;

            return state.time.uniqueNow(max);
        }

        public void addListener(TxnId command, TxnId listener)
        {
            Command.addListener(this, command(command), Command.listener(listener));
        }

        @Override
        public Command.Update beginUpdate(Command command)
        {
            return commands.beginUpdate(command.txnId(), command, c -> new Command.Update(this, c));
        }

        @Override
        public void completeUpdate(Command.Update update, Command current, Command updated)
        {
            commands.completeUpdate(update.txnId(), update, current, updated);
        }

        @Override
        public CommandsForKey.Update beginUpdate(CommandsForKey cfk)
        {
            return commandsForKey.beginUpdate(cfk.key(), cfk, c -> new CommandsForKey.Update(this, c));
        }

        @Override
        public void completeUpdate(CommandsForKey.Update update, CommandsForKey current, CommandsForKey updated)
        {
            commandsForKey.completeUpdate(update.key(), update, current, updated);
        }

        public void finishOperation()
        {
            commands.completeAndUpdateGlobalState(state.commands);
            commandsForKey.completeAndUpdateGlobalState(state.commandsForKey);
        }

        @Override
        public CommandsForKey.CommandLoader<?> cfkLoader()
        {
            return commandStore();
        }
    }

    final InMemoryState state;
    private InMemorySafeStore current;

    public InMemoryCommandStore(int id, int generation, int shardIndex, int numShards, NodeTimeService time, Agent agent, DataStore store, ProgressLog.Factory progressLogFactory, RangesForEpoch rangesForEpoch)
    {
        super(id, generation, shardIndex, numShards);
        this.state = new InMemoryState(time, agent, store, progressLogFactory.create(this), rangesForEpoch, this);
    }

    public abstract boolean containsCommand(TxnId txnId);

    public abstract Command command(TxnId txnId);

    @Override
    public Command loadForCFK(TxnId data)
    {
        InMemorySafeStore safeStore = current;
        Command result;
        // simplifies tests
        if (safeStore != null)
        {
            result = safeStore.commands.get(data);
            if (result != null)
                return result;
        }
        result = state.command(data);
        if (result != null)
            return result;
        throw new IllegalStateException("Could not find command for CFK for " + data);
    }

    @Override
    public TxnId saveForCFK(Command command)
    {
        return command.txnId();
    }

    @Override
    public final AsyncChain<Void> execute(PreLoadContext context, Consumer<? super SafeCommandStore> consumer)
    {
        return submit(context, i -> { consumer.accept(i); return null; });
    }

    protected InMemorySafeStore createCommandStore(InMemoryState state, PreLoadContext context)
    {
        return new InMemorySafeStore(state, context);
    }

    @Override
    public SafeCommandStore beginOperation(PreLoadContext context)
    {
        if (current != null)
            throw new IllegalStateException("Another operation is in progress or it's store was not cleared");
        current = createCommandStore(state, context);
        return current;
    }

    @Override
    public void completeOperation(SafeCommandStore store)
    {
        if (store != current)
            throw new IllegalStateException("This operation has already been cleared");
        current.finishOperation();
        current = null;
    }

    public static class Synchronized extends InMemoryCommandStore
    {
        Runnable active = null;
        final Queue<Runnable> queue = new ConcurrentLinkedQueue<>();

        public Synchronized(int id, int generation, int shardIndex, int numShards, NodeTimeService time, Agent agent, DataStore store, ProgressLog.Factory progressLogFactory, RangesForEpoch rangesForEpoch)
        {
            super(id, generation, shardIndex, numShards, time, agent, store, progressLogFactory, rangesForEpoch);
        }

        @Override
        public synchronized boolean containsCommand(TxnId txnId)
        {
            return state.commands.containsKey(txnId);
        }

        @Override
        public synchronized Command command(TxnId txnId)
        {
            return state.commands.get(txnId);
        }

        @Override
        public Agent agent()
        {
            return state.agent;
        }

        private synchronized void maybeRun()
        {
            if (active != null)
                return;

            active = queue.poll();
            while (active != null)
            {
                try
                {
                    active.run();
                }
                catch (Throwable t)
                {
                    logger.error("Uncaught exception", t);
                }
                active = queue.poll();
            }
        }

        private void enqueueAndRun(Runnable runnable)
        {
            boolean result = queue.add(runnable);
            if (!result)
                throw new IllegalStateException("could not add item to queue");
            maybeRun();
        }

        @Override
        public <T> AsyncChain<T> submit(PreLoadContext context, Function<? super SafeCommandStore, T> function)
        {
            return new AsyncChains.Head<T>()
            {
                @Override
                public void begin(BiConsumer<? super T, Throwable> callback)
                {
                    enqueueAndRun(() -> executeSync(context, function, callback));
                }
            };
        }


        private synchronized <T> void executeSync(PreLoadContext context, Function<? super SafeCommandStore, T> function, BiConsumer<? super T, Throwable> callback)
        {
            CommandStore.executeInContext(this, context, function, callback);
        }

        @Override
        public void shutdown() {}
    }

    public static class SingleThread extends InMemoryCommandStore
    {
        private final AtomicReference<Thread> expectedThread = new AtomicReference<>();
        private final ExecutorService executor;

        public SingleThread(int id, int generation, int shardIndex, int numShards, NodeTimeService time, Agent agent, DataStore store, ProgressLog.Factory progressLogFactory, RangesForEpoch rangesForEpoch)
        {
            super(id, generation, shardIndex, numShards, time, agent, store, progressLogFactory, rangesForEpoch);
            this.executor = Executors.newSingleThreadExecutor(r -> {
                Thread thread = new Thread(r);
                thread.setName(CommandStore.class.getSimpleName() + '[' + time.id() + ':' + shardIndex + ']');
                return thread;
            });
        }

        void assertThread()
        {
            Thread current = Thread.currentThread();
            Thread expected;
            while (true)
            {
                expected = expectedThread.get();
                if (expected != null)
                    break;
                expectedThread.compareAndSet(null, Thread.currentThread());
            }
            if (expected != current)
                throw new IllegalStateException(String.format("Command store called from the wrong thread. Expected %s, got %s", expected, current));
        }

        @Override
        public boolean containsCommand(TxnId txnId)
        {
            assertThread();
            return state.commands.containsKey(txnId);
        }

        @Override
        public Command command(TxnId txnId)
        {
            assertThread();
            return state.commands.get(txnId);
        }

        @Override
        public Agent agent()
        {
            return state.agent;
        }

        @Override
        public <T> AsyncChain<T> submit(PreLoadContext context, Function<? super SafeCommandStore, T> function)
        {
            return AsyncChains.ofCallable(executor, () -> CommandStore.executeInContext(this, context, function));
        }

        @Override
        public void shutdown()
        {
            executor.shutdown();
        }
    }

    public static class Debug extends SingleThread
    {
        private class DebugInMemorySafeStore extends InMemorySafeStore
        {
            public DebugInMemorySafeStore(InMemoryState state, PreLoadContext context)
            {
                super(state, context);
            }

            @Override
            public Command ifPresent(TxnId txnId)
            {
                assertThread();
                return super.ifPresent(txnId);
            }

            @Override
            public Command ifLoaded(TxnId txnId)
            {
                assertThread();
                return super.ifLoaded(txnId);
            }

            @Override
            public Command command(TxnId txnId)
            {
                assertThread();
                return super.command(txnId);
            }

            @Override
            public CommandsForKey commandsForKey(Key key)
            {
                assertThread();
                return super.commandsForKey(key);
            }

            @Override
            public CommandsForKey maybeCommandsForKey(Key key)
            {
                assertThread();
                return super.maybeCommandsForKey(key);
            }

            @Override
            public void addAndInvokeListener(TxnId txnId, TxnId listenerId)
            {
                assertThread();
                super.addAndInvokeListener(txnId, listenerId);
            }

            @Override
            public InMemoryCommandStore commandStore()
            {
                assertThread();
                return super.commandStore();
            }

            @Override
            public DataStore dataStore()
            {
                assertThread();
                return super.dataStore();
            }

            @Override
            public Agent agent()
            {
                assertThread();
                return super.agent();
            }

            @Override
            public ProgressLog progressLog()
            {
                assertThread();
                return super.progressLog();
            }

            @Override
            public RangesForEpoch ranges()
            {
                assertThread();
                return super.ranges();
            }

            @Override
            public long latestEpoch()
            {
                assertThread();
                return super.latestEpoch();
            }

            @Override
            public Timestamp maxConflict(Keys keys)
            {
                assertThread();
                return super.maxConflict(keys);
            }

            @Override
            public void addListener(TxnId command, TxnId listener)
            {
                assertThread();
                super.addListener(command, listener);
            }

            @Override
            public Command.Update beginUpdate(Command command)
            {
                assertThread();
                return super.beginUpdate(command);
            }

            @Override
            public void completeUpdate(Command.Update update, Command current, Command updated)
            {
                assertThread();
                super.completeUpdate(update, current, updated);
            }

            @Override
            public void finishOperation()
            {
                assertThread();
                super.finishOperation();
            }
        }


        public Debug(int id, int generation, int shardIndex, int numShards, NodeTimeService time, Agent agent, DataStore store, ProgressLog.Factory progressLogFactory, RangesForEpoch rangesForEpoch)
        {
            super(id, generation, shardIndex, numShards, time, agent, store, progressLogFactory, rangesForEpoch);
        }

        @Override
        protected InMemorySafeStore createCommandStore(InMemoryState state, PreLoadContext context)
        {
            return new DebugInMemorySafeStore(state, context);
        }
    }

    public static InMemoryState inMemory(CommandStore unsafeStore)
    {
        return (unsafeStore instanceof Synchronized) ? ((Synchronized) unsafeStore).state : ((SingleThread) unsafeStore).state;
    }

    public static InMemoryState inMemory(SafeCommandStore safeStore)
    {
        return inMemory(safeStore.commandStore());
    }
}
