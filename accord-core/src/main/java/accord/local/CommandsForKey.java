package accord.local;

import accord.api.Key;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSortedMap;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Stream;

import static accord.local.CommandsForKey.CommandTimeseries.TestDep.ANY_DEPS;
import static accord.local.CommandsForKey.CommandTimeseries.TestDep.WITHOUT;
import static accord.local.CommandsForKey.CommandTimeseries.TestKind.RorWs;
import static accord.primitives.Txn.Kind.WRITE;
import static accord.utils.Utils.*;

public class CommandsForKey extends Invalidatable
{
    public interface CommandLoader<D>
    {
        Command loadForCFK(D data);
        D saveForCFK(Command command);
    }

    public static class CommandTimeseries<T, D>
    {
        public enum Kind { UNCOMMITTED, COMMITTED_BY_ID, COMMITTED_BY_EXECUTE_AT }
        public enum TestDep { WITH, WITHOUT, ANY_DEPS }
        public enum TestStatus
        {
            IS, HAS_BEEN, ANY_STATUS;
            public static boolean test(Status test, TestStatus predicate, Status param)
            {
                return predicate == ANY_STATUS || (predicate == IS ? test == param : test.hasBeen(param));
            }
        }
        public enum TestKind { Ws, RorWs}

        private static <T, D> Stream<T> before(Function<Command, T> map, Function<D, Command> loader, NavigableMap<Timestamp, D> commands, @Nonnull Timestamp timestamp, @Nonnull TestKind testKind, @Nonnull TestDep testDep, @Nullable TxnId depId, @Nonnull TestStatus testStatus, @Nullable Status status)
        {
            return commands.headMap(timestamp, false).values().stream()
                    .map(loader)
                    .filter(cmd -> testKind == RorWs || cmd.partialTxn().kind() == WRITE)
                    .filter(cmd -> testDep == ANY_DEPS || (cmd.partialDeps().contains(depId) ^ (testDep == WITHOUT)))
                    .filter(cmd -> TestStatus.test(cmd.status(), testStatus, status))
                    .map(map);
        }

        private static <T, D> Stream<T> after(Function<Command, T> map, Function<D, Command> loader, NavigableMap<Timestamp, D> commands, @Nonnull Timestamp timestamp, @Nonnull TestKind testKind, @Nonnull TestDep testDep, @Nullable TxnId depId, @Nonnull TestStatus testStatus, @Nullable Status status)
        {
            return commands.tailMap(timestamp, false).values().stream()
                    .map(loader)
                    .filter(cmd -> testKind == RorWs || cmd.partialTxn().kind() == WRITE)
                    .filter(cmd -> testDep == ANY_DEPS || (cmd.partialDeps().contains(depId) ^ (testDep == WITHOUT)))
                    .filter(cmd -> TestStatus.test(cmd.status(), testStatus, status))
                    .map(map);
        }

        protected final Function<Command, T> map;
        protected final CommandLoader<D> loader;
        protected final ImmutableSortedMap<Timestamp, D> commands;

        public CommandTimeseries(Update<T, D> builder)
        {
            this.map = builder.map;
            this.loader = builder.loader;
            this.commands = ensureSortedImmutable(builder.commands);
        }

        public CommandTimeseries(Function<Command, T> map, CommandLoader<D> loader)
        {
            this.map = map;
            this.loader = loader;
            this.commands = ImmutableSortedMap.of();
        }

        public D get(Timestamp key)
        {
            return commands.get(key);
        }

        public boolean isEmpty()
        {
            return commands.isEmpty();
        }

        public Stream<T> before(@Nonnull Timestamp timestamp, @Nonnull TestKind testKind, @Nonnull TestDep testDep, @Nullable TxnId depId, @Nonnull TestStatus testStatus, @Nullable Status status)
        {
            return before(map, loader::loadForCFK, commands, timestamp, testKind, testDep, depId, testStatus, status);
        }

        public Stream<T> after(@Nonnull Timestamp timestamp, @Nonnull TestKind testKind, @Nonnull TestDep testDep, @Nullable TxnId depId, @Nonnull TestStatus testStatus, @Nullable Status status)
        {
            return after(map, loader::loadForCFK, commands, timestamp, testKind, testDep, depId, testStatus, status);
        }

        public Stream<T> between(Timestamp min, Timestamp max)
        {
            return commands.subMap(min, true, max, true).values().stream().map(loader::loadForCFK).map(map);
        }

        public Stream<T> all()
        {
            return commands.values().stream().map(loader::loadForCFK).map(map);
        }

        public static class Update<T, D>
        {
            protected Function<Command, T> map;
            protected CommandLoader<D> loader;
            protected NavigableMap<Timestamp, D> commands;

            public Update(Function<Command, T> map, CommandLoader<D> loader)
            {
                this.map = map;
                this.loader = loader;
                this.commands = new TreeMap<>();
            }

            public Update(CommandTimeseries<T, D> timeseries)
            {
                this.map = timeseries.map;
                this.loader = timeseries.loader;
                this.commands = timeseries.commands;
            }

            public void add(Timestamp timestamp, Command command)
            {
                if (commands.containsKey(timestamp) && !commands.get(timestamp).equals(command))
                    throw new IllegalStateException(String.format("Attempting to overwrite command at timestamp %s %s with %s.",
                                                                  timestamp, commands.get(timestamp), command));
                commands = ensureSortedMutable(commands);
                commands.put(timestamp, loader.saveForCFK(command));
            }

            public void remove(Timestamp timestamp)
            {
                commands = ensureSortedMutable(commands);
                commands.remove(timestamp);
            }

            CommandTimeseries<T, D> build()
            {
                return new CommandTimeseries<>(this);
            }
        }
    }

    private static class Listener implements CommandListener
    {
        protected final Key listenerKey;

        public Listener(Key listenerKey)
        {
            Preconditions.checkArgument(listenerKey != null);
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

        @Override
        public void onChange(SafeCommandStore safeStore, TxnId txnId)
        {
            CommandsForKeys.listenerUpdate(safeStore, safeStore.commandsForKey(listenerKey), safeStore.command(txnId));
        }

        @Override
        public PreLoadContext listenerPreLoadContext(TxnId caller)
        {
            return PreLoadContext.contextFor(caller, listOf(listenerKey));
        }
    }

    public static CommandListener listener(Key key)
    {
        return new Listener(key);
    }


    public interface TxnIdWithExecuteAt
    {
        TxnId txnId();
        Timestamp executeAt();

        class Immutable implements TxnIdWithExecuteAt
        {
            private final TxnId txnId;
            private final Timestamp executeAt;

            public Immutable(TxnId txnId, Timestamp executeAt)
            {
                this.txnId = txnId;
                this.executeAt = executeAt;
            }

            @Override
            public TxnId txnId()
            {
                return txnId;
            }

            @Override
            public Timestamp executeAt()
            {
                return executeAt;
            }
        }

        static TxnIdWithExecuteAt from(Command command)
        {
            return new TxnIdWithExecuteAt.Immutable(command.txnId(), command.executeAt());
        }
    }

    private final Key key;
    private final Timestamp max;
    private final CommandTimeseries<TxnIdWithExecuteAt, ?> uncommitted;
    private final CommandTimeseries<TxnId, ?> committedById;
    private final CommandTimeseries<TxnId, ?> committedByExecuteAt;

    public CommandsForKey(Key key, CommandLoader<?> loader)
    {
        this.key = key;
        this.max = Timestamp.NONE;
        this.uncommitted = new CommandTimeseries<>(TxnIdWithExecuteAt::from, loader);
        this.committedById = new CommandTimeseries<>(Command::txnId, loader);
        this.committedByExecuteAt = new CommandTimeseries<>(Command::txnId, loader);
    }

    public CommandsForKey(Update builder)
    {
        this.key = builder.key;
        this.max = builder.max;
        this.uncommitted = builder.uncommitted.build();
        this.committedById = builder.committedById.build();
        this.committedByExecuteAt = builder.committedByExecuteAt.build();
    }

    public Key key()
    {
        checkNotInvalidated();
        return key;
    }

    public Timestamp max()
    {
        checkNotInvalidated();
        return max;
    }

    public CommandTimeseries<? extends TxnIdWithExecuteAt, ?> uncommitted()
    {
        checkNotInvalidated();
        return uncommitted;
    }

    public CommandTimeseries<TxnId, ?> committedById()
    {
        checkNotInvalidated();
        return committedById;
    }

    public CommandTimeseries<TxnId, ?> committedByExecuteAt()
    {
        checkNotInvalidated();
        return committedByExecuteAt;
    }

    public static class Update
    {
        private final SafeCommandStore safeStore;
        private boolean completed = false;
        private final Key key;
        private final CommandsForKey original;
        private Timestamp max;
        private final CommandTimeseries.Update<TxnIdWithExecuteAt, ?> uncommitted;
        private final CommandTimeseries.Update<TxnId, ?> committedById;
        private final CommandTimeseries.Update<TxnId, ?> committedByExecuteAt;

        protected  <T, D> CommandTimeseries.Update<T, D> seriesBuilder(Function<Command, T> map, CommandLoader<D> loader, CommandTimeseries.Kind kind)
        {
            return new CommandTimeseries.Update<>(map, loader);
        }

        protected  <T, D> CommandTimeseries.Update<T, D> seriesBuilder(CommandTimeseries<T, D> series, CommandTimeseries.Kind kind)
        {
            return new CommandTimeseries.Update<>(series);
        }

        public Update(SafeCommandStore safeStore, CommandsForKey original)
        {
            this.safeStore = safeStore;
            this.original = original;
            this.key = original.key;
            this.max = original.max;
            this.uncommitted = seriesBuilder(original.uncommitted, CommandTimeseries.Kind.UNCOMMITTED);
            this.committedById = seriesBuilder(original.committedById, CommandTimeseries.Kind.COMMITTED_BY_ID);
            this.committedByExecuteAt = seriesBuilder(original.committedByExecuteAt, CommandTimeseries.Kind.COMMITTED_BY_EXECUTE_AT);
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

        void addUncommitted(Command command)
        {
            checkNotCompleted();
            uncommitted.add(command.txnId(), command);
        }

        void removeUncommitted(Command command)
        {
            checkNotCompleted();
            uncommitted.remove(command.txnId());
        }

        void addCommitted(Command command)
        {
            checkNotCompleted();
            committedById.add(command.txnId(), command);
            committedByExecuteAt.add(command.executeAt(), command);
        }

        public CommandsForKey complete()
        {
            checkNotCompleted();
            CommandsForKey updated = new CommandsForKey(this);
            safeStore.completeUpdate(this, original, updated);
            completed = true;
            return updated;
        }
    }
}
