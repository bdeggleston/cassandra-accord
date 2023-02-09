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

import accord.api.Key;
import accord.local.*;
import accord.primitives.*;
import com.google.common.collect.ImmutableSortedMap;

import javax.annotation.Nullable;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static accord.local.SafeCommandStore.TestDep.ANY_DEPS;
import static accord.local.SafeCommandStore.TestDep.WITH;
import static accord.local.SafeCommandStore.TestKind.Ws;
import static accord.local.Status.PreAccepted;
import static accord.local.Status.PreCommitted;
import static accord.utils.Utils.*;

public class CommandsForKey extends ImmutableState
{
    // sentinel value to indicate a cfk requested in a preexecute context was not found
    // should not escape the safe command store
    public static final CommandsForKey EMPTY = new CommandsForKey(null, null)
    {
        @Override public Key key() { throw new IllegalStateException("Attempting to access EMPTY sentinel values"); }
        @Override public Timestamp max() { throw new IllegalStateException("Attempting to access EMPTY sentinel values"); }
        @Override public Timestamp lastExecutedTimestamp() { throw new IllegalStateException("Attempting to access EMPTY sentinel values"); }
        @Override public long lastExecutedMicros() { throw new IllegalStateException("Attempting to access EMPTY sentinel values"); }
        @Override public Timestamp lastWriteTimestamp() { throw new IllegalStateException("Attempting to access EMPTY sentinel values"); }
        @Override public CommandTimeseries<?> byId() { throw new IllegalStateException("Attempting to access EMPTY sentinel values"); }
        @Override public CommandTimeseries<?> byExecuteAt() { throw new IllegalStateException("Attempting to access EMPTY sentinel values"); }

        @Override
        public String toString()
        {
            return "CommandsForKey[EMPTY]";
        }
    };

    static
    {
        EMPTY.markInvalidated();
    }

    public static class SerializerSupport
    {
        public static CommandsForKey.Listener listener(Key key)
        {
            return new CommandsForKey.Listener(key);
        }

        public static  <D> CommandsForKey create(Key key, Timestamp max,
                                                 Timestamp lastExecutedTimestamp, long lastExecutedMicros, Timestamp lastWriteTimestamp,
                                                 CommandLoader<D> loader,
                                                 ImmutableSortedMap<Timestamp, D> byId,
                                                 ImmutableSortedMap<Timestamp, D> byExecuteAt)
        {
            return new CommandsForKey(key, max, lastExecutedTimestamp, lastExecutedMicros, lastWriteTimestamp, loader, byId, byExecuteAt);
        }
    }

    public interface CommandLoader<D>
    {
        D saveForCFK(Command command);

        TxnId txnId(D data);
        Timestamp executeAt(D data);
        SaveStatus saveStatus(D data);
        PartialDeps partialDeps(D data);

        default Status status(D data)
        {
            return saveStatus(data).status;
        }

        default Status.Known known(D data)
        {
            return saveStatus(data).known;
        }
    }

    public static class CommandTimeseries<D>
    {
        public enum TestTimestamp {BEFORE, AFTER}

        private final Key key;
        protected final CommandLoader<D> loader;
        public final ImmutableSortedMap<Timestamp, D> commands;

        public CommandTimeseries(Update<D> builder)
        {
            this.key = builder.key;
            this.loader = builder.loader;
            this.commands = ensureSortedImmutable(builder.commands);
        }

        CommandTimeseries(Key key, CommandLoader<D> loader, ImmutableSortedMap<Timestamp, D> commands)
        {
            this.key = key;
            this.loader = loader;
            this.commands = commands;
        }

        public CommandTimeseries(Key key, CommandLoader<D> loader)
        {
            this(key, loader, ImmutableSortedMap.of());
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            CommandTimeseries<?> that = (CommandTimeseries<?>) o;
            return key.equals(that.key) && loader.equals(that.loader) && commands.equals(that.commands);
        }

        @Override
        public int hashCode()
        {
            int hash = 1;
            hash = 31 * hash + Objects.hashCode(key);
            hash = 31 * hash + Objects.hashCode(loader);
            hash = 31 * hash + Objects.hashCode(commands);
            return hash;
        }

        public D get(Timestamp key)
        {
            return commands.get(key);
        }

        public boolean isEmpty()
        {
            return commands.isEmpty();
        }

        /**
         * All commands before/after (exclusive of) the given timestamp
         * <p>
         * Note that {@code testDep} applies only to commands that know at least proposed deps; if specified any
         * commands that do not know any deps will be ignored.
         * <p>
         * TODO (expected, efficiency): TestDep should be asynchronous; data should not be kept memory-resident as only used for recovery
         */
        public <T> T mapReduce(SafeCommandStore.TestKind testKind, TestTimestamp testTimestamp, Timestamp timestamp,
                               SafeCommandStore.TestDep testDep, @Nullable TxnId depId,
                               @Nullable Status minStatus, @Nullable Status maxStatus,
                               SafeCommandStore.CommandFunction<T, T> map, T initialValue, T terminalValue)
        {

            for (D data : (testTimestamp == TestTimestamp.BEFORE ? commands.headMap(timestamp, false) : commands.tailMap(timestamp, false)).values())
            {
                TxnId txnId = loader.txnId(data);
                Timestamp executeAt = loader.executeAt(data);
                SaveStatus status = loader.saveStatus(data);
                PartialDeps deps = loader.partialDeps(data);
                if (testKind == Ws && txnId.isRead()) continue;
                // If we don't have any dependencies, we treat a dependency filter as a mismatch
                if (testDep != ANY_DEPS && (!status.known.deps.hasProposedOrDecidedDeps() || (deps.contains(depId) != (testDep == WITH))))
                    continue;
                if (minStatus != null && minStatus.compareTo(status.status) > 0)
                    continue;
                if (maxStatus != null && maxStatus.compareTo(status.status) < 0)
                    continue;
                initialValue = map.apply(key, txnId, executeAt, initialValue);
                if (initialValue.equals(terminalValue))
                    break;
            }
            return initialValue;
        }

        Stream<TxnId> between(Timestamp min, Timestamp max, Predicate<Status> statusPredicate)
        {
            return commands.subMap(min, true, max, true).values().stream()
                    .filter(d -> statusPredicate.test(loader.status(d))).map(loader::txnId);
        }

        public Stream<D> all()
        {
            return commands.values().stream();
        }

        Update<D> beginUpdate()
        {
            return new Update<>(this);
        }

        public CommandLoader<D> loader()
        {
            return loader;
        }

        public static class Update<D>
        {
            private final Key key;
            protected CommandLoader<D> loader;
            protected NavigableMap<Timestamp, D> commands;

            public Update(Key key, CommandLoader<D> loader)
            {
                this.key = key;
                this.loader = loader;
                this.commands = new TreeMap<>();
            }

            public Update(CommandTimeseries<D> timeseries)
            {
                this.key = timeseries.key;
                this.loader = timeseries.loader;
                this.commands = timeseries.commands;
            }

            public void add(Timestamp timestamp, Command command)
            {
                commands = ensureSortedMutable(commands);
                commands.put(timestamp, loader.saveForCFK(command));
            }

            public void remove(Timestamp timestamp)
            {
                commands = ensureSortedMutable(commands);
                commands.remove(timestamp);
            }

            CommandTimeseries<D> build()
            {
                return new CommandTimeseries<>(this);
            }
        }
    }

    public static class Listener implements CommandListener
    {
        protected final Key listenerKey;

        private Listener(Key listenerKey)
        {
            this.listenerKey = listenerKey;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Listener that = (Listener) o;
            return listenerKey.equals(that.listenerKey);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(listenerKey);
        }

        @Override
        public String toString()
        {
            return "ListenerProxy{" + listenerKey + '}';
        }

        public Key key()
        {
            return listenerKey;
        }

        @Override
        public void onChange(SafeCommandStore safeStore, TxnId txnId)
        {
            CommandsForKeys.listenerUpdate(safeStore, ((AbstractSafeCommandStore) safeStore).commandsForKey(listenerKey), safeStore.command(txnId));
        }

        @Override
        public PreLoadContext listenerPreLoadContext(TxnId caller)
        {
            return PreLoadContext.contextFor(caller, Keys.of(listenerKey));
        }
    }

    public static CommandListener listener(Key key)
    {
        return new Listener(key);
    }

    // TODO (now): add validation that anything inserted into *committedBy* has everything prior in its dependencies
    private final Key key;
    private final Timestamp max;
    private final Timestamp lastExecutedTimestamp;
    private final long lastExecutedMicros;
    private final Timestamp lastWriteTimestamp;
    private final CommandTimeseries<?> byId;
    private final CommandTimeseries<?> byExecuteAt;

    private  <D> CommandsForKey(Key key, Timestamp max,
                              Timestamp lastExecutedTimestamp, long lastExecutedMicros, Timestamp lastWriteTimestamp,
                              CommandLoader<D> loader,
                              ImmutableSortedMap<Timestamp, D> committedById,
                              ImmutableSortedMap<Timestamp, D> committedByExecuteAt)
    {
        this.key = key;
        this.max = max;
        this.lastExecutedTimestamp = lastExecutedTimestamp;
        this.lastExecutedMicros = lastExecutedMicros;
        this.lastWriteTimestamp = lastWriteTimestamp;
        this.byId = new CommandTimeseries<>(key, loader, committedById);
        this.byExecuteAt = new CommandTimeseries<>(key, loader, committedByExecuteAt);
    }

    public <D> CommandsForKey(Key key, CommandLoader<D> loader)
    {
        this.key = key;
        this.max = Timestamp.NONE;
        this.lastExecutedTimestamp = Timestamp.NONE;
        this.lastExecutedMicros = 0;
        this.lastWriteTimestamp = Timestamp.NONE;
        this.byId = new CommandTimeseries<>(key, loader);
        this.byExecuteAt = new CommandTimeseries<>(key, loader);
    }

    public CommandsForKey(Update builder)
    {
        this.key = builder.key;
        this.max = builder.max;
        this.lastExecutedTimestamp = builder.lastExecutedTimestamp;
        this.lastExecutedMicros = builder.lastExecutedMicros;
        this.lastWriteTimestamp = builder.lastWriteTimestamp;
        this.byId = builder.byId.build();
        this.byExecuteAt = builder.byExecuteAt.build();
    }

    @Override
    public String toString()
    {
        return "CommandsForKey@" + System.identityHashCode(this) + '{' + key + '}';
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CommandsForKey that = (CommandsForKey) o;
        return lastExecutedMicros == that.lastExecutedMicros
                && key.equals(that.key)
                && Objects.equals(max, that.max)
                && Objects.equals(lastExecutedTimestamp, that.lastExecutedTimestamp)
                && Objects.equals(lastWriteTimestamp, that.lastWriteTimestamp)
                && byId.equals(that.byId)
                && byExecuteAt.equals(that.byExecuteAt);
    }

    @Override
    public int hashCode()
    {
        int hash = 1;
        hash = 31 * hash + Objects.hashCode(key);
        hash = 31 * hash + Objects.hashCode(max);
        hash = 31 * hash + Objects.hashCode(lastExecutedTimestamp);
        hash = 31 * hash + Long.hashCode(lastExecutedMicros);
        hash = 31 * hash + Objects.hashCode(lastWriteTimestamp);
        hash = 31 * hash + byId.hashCode();
        hash = 31 * hash + byExecuteAt.hashCode();
        return hash;
    }

    public Key key()
    {
        checkCanReadFrom();
        return key;
    }

    public Timestamp max()
    {
        checkCanReadFrom();
        return max;
    }

    public Timestamp lastExecutedTimestamp()
    {
        return lastExecutedTimestamp;
    }

    public long lastExecutedMicros()
    {
        return lastExecutedMicros;
    }

    public Timestamp lastWriteTimestamp()
    {
        return lastWriteTimestamp;
    }

    public CommandTimeseries<?> byId()
    {
        return byId;
    }

    public CommandTimeseries<?> byExecuteAt()
    {
        return byExecuteAt;
    }

    public void forWitnessed(Timestamp minTs, Timestamp maxTs, Consumer<TxnId> consumer)
    {
        byId.between(minTs, maxTs, status -> status.hasBeen(PreAccepted)).forEach(consumer);
        byExecuteAt.between(minTs, maxTs, status -> status.hasBeen(PreCommitted)).forEach(consumer);
    }

    private static long getTimestampMicros(Timestamp timestamp)
    {
        return timestamp.hlc();
    }


    private void validateExecuteAtTime(Timestamp executeAt, boolean isForWriteTxn)
    {
        if (executeAt.compareTo(lastWriteTimestamp) < 0)
            throw new IllegalArgumentException(String.format("%s is less than the most recent write timestamp %s", executeAt, lastWriteTimestamp));

        int cmp = executeAt.compareTo(lastExecutedTimestamp);
        // execute can be in the past if it's for a read and after the most recent write
        if (cmp == 0 || (!isForWriteTxn && cmp < 0))
            return;
        if (cmp < 0)
            throw new IllegalArgumentException(String.format("%s is less than the most recent executed timestamp %s", executeAt, lastExecutedTimestamp));
        else
            throw new IllegalArgumentException(String.format("%s is greater than the most recent executed timestamp, cfk should be updated", executeAt, lastExecutedTimestamp));
    }

    public int nowInSecondsFor(Timestamp executeAt, boolean isForWriteTxn)
    {
        validateExecuteAtTime(executeAt, isForWriteTxn);
        // we use the executeAt time instead of the monotonic database timestamp to prevent uneven
        // ttl expiration in extreme cases, ie 1M+ writes/second to a key causing timestamps to overflow
        // into the next second on some keys and not others.
        return Math.toIntExact(TimeUnit.MICROSECONDS.toSeconds(getTimestampMicros(lastExecutedTimestamp)));
    }

    public long timestampMicrosFor(Timestamp executeAt, boolean isForWriteTxn)
    {
        validateExecuteAtTime(executeAt, isForWriteTxn);
        return lastExecutedMicros;
    }

    public static class Update
    {
        private final SafeCommandStore safeStore;
        private boolean completed = false;
        private final Key key;
        private final CommandsForKey original;
        private Timestamp max;
        private Timestamp lastExecutedTimestamp;
        private long lastExecutedMicros;
        private Timestamp lastWriteTimestamp;
        private final CommandTimeseries.Update<?> byId;
        private final CommandTimeseries.Update<?> byExecuteAt;

        public Update(SafeCommandStore safeStore, CommandsForKey original)
        {
            original.checkCanUpdate();
            this.safeStore = safeStore;
            this.original = original;
            this.key = original.key;
            this.max = original.max;
            this.lastExecutedTimestamp = original.lastExecutedTimestamp;
            this.lastExecutedMicros = original.lastExecutedMicros;
            this.lastWriteTimestamp = original.lastWriteTimestamp;
            this.byId = original.byId.beginUpdate();
            this.byExecuteAt = original.byId.beginUpdate();
        }

        private void checkNotCompleted()
        {
            if (completed)
                throw new IllegalStateException(this + " has been completed");
        }

        public Key key()
        {
            return key;
        }

        public void updateMax(Timestamp timestamp)
        {
            checkNotCompleted();
            max = Timestamp.max(max, timestamp);
        }

        public CommandTimeseries.Update<?> byId()
        {
            checkNotCompleted();
            return byId;
        }

        public CommandTimeseries.Update<?> byExecuteAt()
        {
            checkNotCompleted();
            return byExecuteAt;
        }

        void updateLastExecutionTimestamps(Timestamp executeAt, boolean isForWriteTxn)
        {
            long micros = getTimestampMicros(executeAt);
            long lastMicros = lastExecutedMicros;

            lastExecutedTimestamp = executeAt;
            lastExecutedMicros = Math.max(micros, lastMicros + 1);
            if (isForWriteTxn)
                lastWriteTimestamp = executeAt;
        }

        public CommandsForKey complete()
        {
            checkNotCompleted();
            CommandsForKey updated = new CommandsForKey(this);
            if (original != null)
                original.markSuperseded();
            updated.markActive();
            safeStore.completeUpdate(this, original, updated);
            completed = true;
            return updated;
        }
    }

    public static CommandsForKey updateLastExecutionTimestamps(CommandsForKey current, SafeCommandStore safeStore, Timestamp executeAt, boolean isForWriteTxn)
    {
        Timestamp lastWrite = current.lastWriteTimestamp;

        if (executeAt.compareTo(lastWrite) < 0)
            throw new IllegalArgumentException(String.format("%s is less than the most recent write timestamp %s", executeAt, lastWrite));

        Timestamp lastExecuted = current.lastExecutedTimestamp;
        int cmp = executeAt.compareTo(lastExecuted);
        // execute can be in the past if it's for a read and after the most recent write
        if (cmp == 0 || (!isForWriteTxn && cmp < 0))
            return current;
        if (cmp < 0)
            throw new IllegalArgumentException(String.format("%s is less than the most recent executed timestamp %s", executeAt, lastExecuted));

        Update update = safeStore.beginUpdate(current);
        update.updateLastExecutionTimestamps(executeAt, isForWriteTxn);
        return update.complete();
    }
}