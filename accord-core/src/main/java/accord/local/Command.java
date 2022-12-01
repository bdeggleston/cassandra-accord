package accord.local;

import accord.api.Data;
import accord.api.Key;
import accord.api.Result;
import accord.api.RoutingKey;
import accord.primitives.*;
import accord.utils.Utils;
import accord.utils.async.AsyncChain;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.ImmutableSortedSet;

import java.util.*;

import static accord.local.Status.Durability.Local;
import static accord.local.Status.Durability.NotDurable;
import static accord.local.Status.Known.*;
import static accord.utils.Utils.*;
import static java.lang.String.format;

public abstract class Command extends Invalidatable
{
    static PreLoadContext contextForCommand(Command command)
    {
        Preconditions.checkState(command.hasBeen(Status.PreAccepted) && command.partialTxn() != null);
        return command instanceof PreLoadContext ? (PreLoadContext) command : PreLoadContext.contextFor(command.txnId(), command.partialTxn().keys());
    }

    private static Status.Durability durability(Status.Durability durability, SaveStatus status)
    {
        if (status.compareTo(SaveStatus.PreApplied) >= 0 && durability == NotDurable)
            return Local; // not necessary anywhere, but helps for logical consistency
        return durability;
    }

    public interface CommonAttributes
    {
        TxnId txnId();
        Status.Durability durability();
        RoutingKey homeKey();
        RoutingKey progressKey();
        AbstractRoute route();
        PartialTxn partialTxn();
        PartialDeps partialDeps();
        ImmutableSet<CommandListener> listeners();
    }

    public static class SerializerSupport
    {

    }

    private static SaveStatus validateCommandClass(SaveStatus status, Class<?> expected, Class<?> actual)
    {
        if (actual != expected)
        {
            throw new IllegalStateException(format("Cannot instantiate %s for status %s. %s expected",
                                                   actual.getSimpleName(), status, expected.getSimpleName()));
        }
        return status;
    }

    private static SaveStatus validateCommandClass(SaveStatus status, Class<?> klass)
    {
        switch (status)
        {
            case NotWitnessed:
                return validateCommandClass(status, NotWitnessed.class, klass);
            case PreAccepted:
                return validateCommandClass(status, Preaccepted.class, klass);
            case AcceptedInvalidate:
            case AcceptedInvalidateWithDefinition:
            case Accepted:
            case AcceptedWithDefinition:
                return validateCommandClass(status, Accepted.class, klass);
            case Committed:
            case ReadyToExecute:
            case Invalidated:
                return validateCommandClass(status, Committed.class, klass);
            case PreApplied:
            case Applied:
                return validateCommandClass(status, Executed.class, klass);
            default:
                throw new IllegalStateException("Unhandled status " + status);
        }
    }

    public static Command addListener(SafeCommandStore safeStore, Command command, CommandListener listener)
    {
        return safeStore.beginUpdate(command).addListener(listener).updateAttributes();
    }

    public static Command removeListener(SafeCommandStore safeStore, Command command, CommandListener listener)
    {
        return safeStore.beginUpdate(command).removeListener(listener).updateAttributes();
    }

    public static Committed updateWaitingOn(SafeCommandStore safeStore, Committed command, WaitingOn.Update waitingOn)
    {
        if (!waitingOn.hasChanges())
            return command;

        Update update = safeStore.beginUpdate(command);
        Committed updated =  command instanceof Executed ?
                Executed.update(command.asExecuted(), update, waitingOn.build()) :
                Committed.update(command, update, waitingOn.build());
        return update.complete(updated);
    }

    private static class Listener implements CommandListener
    {
        protected final TxnId listenerId;

        public Listener(TxnId listenerId)
        {
            Preconditions.checkState(listenerId != null);
            this.listenerId = listenerId;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Listener that = (Listener) o;
            return listenerId.equals(that.listenerId);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(listenerId);
        }

        @Override
        public String toString()
        {
            return "ListenerProxy{" + listenerId + '}';
        }

        @Override
        public void onChange(SafeCommandStore safeStore, TxnId txnId)
        {
            Commands.listenerUpdate(safeStore, safeStore.command(listenerId), safeStore.command(txnId));
        }

        @Override
        public PreLoadContext listenerPreLoadContext(TxnId caller)
        {
            return PreLoadContext.contextFor(Utils.listOf(listenerId, caller), Collections.emptyList());
        }
    }

    public static CommandListener listener(TxnId txnId)
    {
        return new Listener(txnId);
    }

    private final TxnId txnId;
    private final SaveStatus status;
    private final Status.Durability durability;
    private final RoutingKey homeKey;
    private final RoutingKey progressKey;
    private final AbstractRoute route;
    private final ImmutableSet<CommandListener> listeners;

    private Command(TxnId txnId, SaveStatus status, Status.Durability durability, RoutingKey homeKey, RoutingKey progressKey, AbstractRoute route, ImmutableSet<CommandListener> listeners)
    {
        this.txnId = txnId;
        this.status = validateCommandClass(status, getClass());
        this.durability = durability;
        this.homeKey = homeKey;
        this.progressKey = progressKey;
        this.route = route;
        this.listeners = listeners;
    }

    private Command(CommonAttributes common, SaveStatus status)
    {
        this.txnId = common.txnId();
        this.status = validateCommandClass(status, getClass());
        this.durability = common.durability();
        this.homeKey = common.homeKey();
        this.progressKey = common.progressKey();
        this.route = common.route();
        this.listeners = common.listeners();
    }

    private static boolean isSameClass(Command command, Class<? extends Command> klass)
    {
        return command.getClass() == klass;
    }

    private static void checkSameClass(Command command, Class<? extends Command> klass, String errorMsg)
    {
        if (!isSameClass(command, klass))
            throw new IllegalArgumentException(errorMsg + format(" Expected %s got %s", klass.getSimpleName(), command.getClass().getSimpleName()));
    }

    public TxnId txnId()
    {
        return txnId;
    }

    public final RoutingKey homeKey()
    {
        checkNotInvalidated();
        return homeKey;
    }

    public final RoutingKey progressKey()
    {
        checkNotInvalidated();
        return progressKey;
    }

    public final AbstractRoute route()
    {
        checkNotInvalidated();
        return route;
    }

    public final AbstractRoute someRoute()
    {
        checkNotInvalidated();
        if (route() != null)
            return route();

        if (homeKey() != null)
            return new PartialRoute(KeyRanges.EMPTY, homeKey(), new RoutingKey[0]);

        return null;
    }

    public final RoutingKeys maxRoutingKeys()
    {
        checkNotInvalidated();
        AbstractRoute route = someRoute();
        if (route == null)
            return null;

        return route.with(route.homeKey);
    }

    public PreLoadContext contextForSelf()
    {
        checkNotInvalidated();
        return contextForCommand(this);
    }

    public Status.Durability durability()
    {
        checkNotInvalidated();
        return durability(durability, saveStatus());
    }

    public ImmutableSet<CommandListener> listeners()
    {
        checkNotInvalidated();
        return listeners;
    }

    public abstract Timestamp executeAt();
    public abstract Ballot promised();
    public abstract Ballot accepted();
    public abstract PartialTxn partialTxn();
    public abstract PartialDeps partialDeps();

    public final SaveStatus saveStatus()
    {
        checkNotInvalidated();
        return status;
    }

    public final Status status()
    {
        checkNotInvalidated();
        return saveStatus().status;
    }

    public final Status.Known known()
    {
        checkNotInvalidated();
        return saveStatus().known;
    }

    public boolean hasBeenWitnessed()
    {
        checkNotInvalidated();
        return partialTxn() != null;
    }

    public final boolean hasBeen(Status status)
    {
        return status().compareTo(status) >= 0;
    }

    public final boolean hasBeen(Status.Known phase)
    {
        return known().compareTo(phase) >= 0;
    }

    public final boolean hasBeen(Status.ExecutionStatus phase)
    {
        return status().execution.compareTo(phase) >= 0;
    }

    public final CommandListener asListener()
    {
        return listener(txnId);
    }

    public final boolean isWitnessed()
    {
        checkNotInvalidated();
        boolean result = status().hasBeen(Status.PreAccepted);
        Preconditions.checkState(result == (this instanceof Preaccepted));
        return result;
    }

    public final Preaccepted asWitnessed()
    {
        checkNotInvalidated();
        return (Preaccepted) this;
    }

    public final boolean isAccepted()
    {
        checkNotInvalidated();
        boolean result = status().hasBeen(Status.Accepted);
        Preconditions.checkState(result == (this instanceof Accepted));
        return result;
    }

    public final Accepted asAccepted()
    {
        checkNotInvalidated();
        return (Accepted) this;
    }

    public final boolean isCommitted()
    {
        checkNotInvalidated();
        boolean result = status().hasBeen(Status.Committed);
        Preconditions.checkState(result == (this instanceof Committed));
        return result;
    }

    public final Committed asCommitted()
    {
        checkNotInvalidated();
        return (Committed) this;
    }

    public final boolean isExecuted()
    {
        checkNotInvalidated();
        boolean result = status().hasBeen(Status.PreApplied);
        Preconditions.checkState(result == (this instanceof Executed));
        return result;
    }

    public final Executed asExecuted()
    {
        checkNotInvalidated();
        return (Executed) this;
    }

    public static final class NotWitnessed extends Command
    {
        NotWitnessed(TxnId txnId, SaveStatus status, Status.Durability durability, RoutingKey homeKey, RoutingKey progressKey, AbstractRoute route, ImmutableSet<CommandListener> listeners)
        {
            super(txnId, status, durability, homeKey, progressKey, route, listeners);
        }

        NotWitnessed(CommonAttributes common, SaveStatus status)
        {
            super(common, status);
        }

        public static NotWitnessed create(TxnId txnId)
        {
            return new NotWitnessed(txnId, SaveStatus.NotWitnessed, NotDurable, null, null, null, null);
        }

        public static NotWitnessed create(CommonAttributes common, SaveStatus status)
        {
            return new NotWitnessed(common, status);
        }

        public static NotWitnessed update(NotWitnessed command, CommonAttributes common)
        {
            checkSameClass(command, NotWitnessed.class, "Cannot update");
            command.checkNotInvalidated();
            Preconditions.checkArgument(command.txnId().equals(common.txnId()));
            return new NotWitnessed(common, command.saveStatus());
        }

        @Override
        public Timestamp executeAt()
        {
            checkNotInvalidated();
            return null;
        }

        @Override
        public Ballot promised()
        {
            checkNotInvalidated();
            return Ballot.ZERO;
        }

        @Override
        public Ballot accepted()
        {
            checkNotInvalidated();
            return Ballot.ZERO;
        }

        @Override
        public PartialTxn partialTxn()
        {
            checkNotInvalidated();
            return null;
        }

        @Override
        public PartialDeps partialDeps()
        {
            checkNotInvalidated();
            return null;
        }
    }

    static class Preaccepted extends Command
    {
        private final Timestamp executeAt;
        private final Ballot promised;


        private final PartialTxn partialTxn;
        private final PartialDeps partialDeps;

        private Preaccepted(CommonAttributes common, SaveStatus status, Timestamp executeAt, Ballot promised)
        {
            super(common, status);
            this.executeAt = executeAt;
            this.promised = promised;
            this.partialTxn = common.partialTxn();
            this.partialDeps = common.partialDeps();
        }

        public static Preaccepted create(CommonAttributes common, Timestamp executeAt, Ballot promised)
        {
            return new Preaccepted(common, SaveStatus.PreAccepted, executeAt, promised);
        }

        public static Preaccepted update(Preaccepted command, CommonAttributes common)
        {
            checkSameClass(command, Preaccepted.class, "Cannot update");
            Preconditions.checkArgument(command.getClass() == Preaccepted.class);
            command.checkNotInvalidated();
            return create(common, command.executeAt(), command.promised());
        }

        @Override
        public Timestamp executeAt()
        {
            checkNotInvalidated();
            return executeAt;
        }

        @Override
        public Ballot promised()
        {
            checkNotInvalidated();
            return promised;
        }

        @Override
        public Ballot accepted()
        {
            checkNotInvalidated();
            return Ballot.ZERO;
        }

        @Override
        public PartialTxn partialTxn()
        {
            checkNotInvalidated();
            return partialTxn;
        }

        @Override
        public PartialDeps partialDeps()
        {
            checkNotInvalidated();
            return partialDeps;
        }
    }

    public static class Accepted extends Preaccepted
    {
        private final Ballot accepted;

        private Accepted(CommonAttributes common, SaveStatus status, Timestamp executeAt, Ballot promised, Ballot accepted)
        {
            super(common, status, executeAt, promised);
            this.accepted = accepted;
        }

        static Accepted create(CommonAttributes common, SaveStatus status, Timestamp executeAt, Ballot promised, Ballot accepted)
        {
            return new Accepted(common, status, executeAt, promised, accepted);
        }

        static Accepted update(Accepted command, CommonAttributes common, SaveStatus status)
        {
            checkSameClass(command, Accepted.class, "Cannot update");
            command.checkNotInvalidated();
            return new Accepted(common, status, command.executeAt(), command.promised(), command.accepted());
        }

        static Accepted update(Accepted command, CommonAttributes common)
        {
            return update(command, common, command.saveStatus());
        }

        @Override
        public Ballot accepted()
        {
            checkNotInvalidated();
            return accepted;
        }
    }

    public static class Committed extends Accepted
    {
        private final ImmutableSortedSet<TxnId> waitingOnCommit;
        private final ImmutableSortedMap<Timestamp, TxnId> waitingOnApply;

        private Committed(CommonAttributes common, SaveStatus status, Timestamp executeAt, Ballot promised, Ballot accepted, ImmutableSortedSet<TxnId> waitingOnCommit, ImmutableSortedMap<Timestamp, TxnId> waitingOnApply)
        {
            super(common, status, executeAt, promised, accepted);
            this.waitingOnCommit = waitingOnCommit;
            this.waitingOnApply = waitingOnApply;
        }

        private Committed(CommonAttributes common, SaveStatus status, Timestamp executeAt, Ballot promised, Ballot accepted, WaitingOn waitingOn)
        {
            this(common, status, executeAt, promised, accepted, waitingOn.waitingOnCommit, waitingOn.waitingOnApply);
        }

        private static Committed update(Committed command, CommonAttributes common, SaveStatus status, ImmutableSortedSet<TxnId> waitingOnCommit, ImmutableSortedMap<Timestamp, TxnId> waitingOnApply)
        {
            checkSameClass(command, Committed.class, "Cannot update");
            return new Committed(common, status, command.executeAt(), command.promised(), command.accepted(), waitingOnCommit, waitingOnApply);
        }

        private static Committed update(Committed command, CommonAttributes common)
        {
            return update(command, common, command.saveStatus(), command.waitingOnCommit(), command.waitingOnApply());
        }

        private static Committed update(Committed command, CommonAttributes common, SaveStatus status)
        {
            return update(command, common, status, command.waitingOnCommit(), command.waitingOnApply());
        }

        private static Committed update(Committed command, CommonAttributes common, WaitingOn waitingOn)
        {
            return update(command, common, command.saveStatus(), waitingOn.waitingOnCommit, waitingOn.waitingOnApply);
        }

        public static Committed create(CommonAttributes common, SaveStatus status, Timestamp executeAt, Ballot promised, Ballot accepted, ImmutableSortedSet<TxnId> waitingOnCommit, ImmutableSortedMap<Timestamp, TxnId> waitingOnApply)
        {
            return new Committed(common, status, executeAt, promised, accepted, waitingOnCommit, waitingOnApply);
        }

        public static Committed create(CommonAttributes common, SaveStatus status, Timestamp executeAt, Ballot promised, Ballot accepted, WaitingOn waitingOn)
        {
            return new Committed(common, status, executeAt, promised, accepted, waitingOn.waitingOnCommit, waitingOn.waitingOnApply);
        }

        public AsyncChain<Data> read(SafeCommandStore safeStore)
        {
            checkNotInvalidated();
            return partialTxn().read(safeStore, this);
        }

        public ImmutableSortedSet<TxnId> waitingOnCommit()
        {
            checkNotInvalidated();
            return waitingOnCommit;
        }

        public boolean isWaitingOnCommit()
        {
            checkNotInvalidated();
            return waitingOnCommit != null && !waitingOnCommit.isEmpty();
        }

        public TxnId firstWaitingOnCommit()
        {
            checkNotInvalidated();
            return isWaitingOnCommit() ? waitingOnCommit.first() : null;
        }

        public ImmutableSortedMap<Timestamp, TxnId> waitingOnApply()
        {
            checkNotInvalidated();
            return waitingOnApply;
        }

        public boolean isWaitingOnApply()
        {
            checkNotInvalidated();
            return waitingOnApply != null && !waitingOnApply.isEmpty();
        }

        public TxnId firstWaitingOnApply()
        {
            checkNotInvalidated();
            return isWaitingOnApply() ? waitingOnApply.firstEntry().getValue() : null;
        }

        public boolean hasBeenWitnessed()
        {
            checkNotInvalidated();
            return partialTxn() != null;
        }

        public boolean isUnableToExecute()
        {
            checkNotInvalidated();
            return isWaitingOnCommit() || isWaitingOnApply();
        }
    }

    public static class Executed extends Committed
    {
        private final Writes writes;
        private final Result result;
        private static SaveStatus validateStatus(SaveStatus status)
        {
            switch (status)
            {
                case PreApplied:
                case Applied:
                    return status;
                default:
                    throw new IllegalArgumentException("Cannot create Accepted command with status " + status);
            }
        }

        public Executed(CommonAttributes common, SaveStatus status, Timestamp executeAt, Ballot promised, Ballot accepted, ImmutableSortedSet<TxnId> waitingOnCommit, ImmutableSortedMap<Timestamp, TxnId> waitingOnApply, Writes writes, Result result)
        {
            super(common, status, executeAt, promised, accepted, waitingOnCommit, waitingOnApply);
            this.writes = writes;
            this.result = result;
        }

        public Executed(CommonAttributes common, SaveStatus status, Timestamp executeAt, Ballot promised, Ballot accepted, WaitingOn waitingOn, Writes writes, Result result)
        {
            super(common, status, executeAt, promised, accepted, waitingOn);
            this.writes = writes;
            this.result = result;
        }

        public static Executed update(Executed command, CommonAttributes common, SaveStatus status, ImmutableSortedSet<TxnId> waitingOnCommit, ImmutableSortedMap<Timestamp, TxnId> waitingOnApply)
        {
            checkSameClass(command, Executed.class, "Cannot update");
            command.checkNotInvalidated();
            return new Executed(common, status, command.executeAt(), command.promised(), command.accepted(), command.waitingOnCommit(), command.waitingOnApply(), command.writes(), command.result());
        }

        public static Executed update(Executed command, CommonAttributes common, SaveStatus status)
        {
            return update(command, common, status, command.waitingOnCommit(), command.waitingOnApply());
        }

        public static Executed update(Executed command, CommonAttributes common, WaitingOn waitingOn)
        {
            return update(command, common, command.saveStatus(), waitingOn.waitingOnCommit, waitingOn.waitingOnApply);
        }

        public static Executed update(Executed command, CommonAttributes common)
        {
            return update(command, common, command.saveStatus());
        }

        public static Executed create(CommonAttributes common, SaveStatus status, Timestamp executeAt, Ballot promised, Ballot accepted, ImmutableSortedSet<TxnId> waitingOnCommit, ImmutableSortedMap<Timestamp, TxnId> waitingOnApply, Writes writes, Result result)
        {
            return new Executed(common, status, executeAt, promised, accepted, waitingOnCommit, waitingOnApply, writes, result);
        }

        public static Executed create(CommonAttributes common, SaveStatus status, Timestamp executeAt, Ballot promised, Ballot accepted, WaitingOn waitingOn, Writes writes, Result result)
        {
            return new Executed(common, status, executeAt, promised, accepted, waitingOn.waitingOnCommit, waitingOn.waitingOnApply, writes, result);
        }

        public Writes writes()
        {
            checkNotInvalidated();
            return writes;
        }

        public Result result()
        {
            checkNotInvalidated();
            return result;
        }
    }

    public static class WaitingOn
    {
        public static final WaitingOn EMPTY = new WaitingOn(ImmutableSortedSet.of(), ImmutableSortedMap.of());
        public final ImmutableSortedSet<TxnId> waitingOnCommit;
        public final ImmutableSortedMap<Timestamp, TxnId> waitingOnApply;

        public WaitingOn(ImmutableSortedSet<TxnId> waitingOnCommit, ImmutableSortedMap<Timestamp, TxnId> waitingOnApply)
        {
            this.waitingOnCommit = waitingOnCommit;
            this.waitingOnApply = waitingOnApply;
        }

        public static class Update
        {
            private boolean hasChanges = false;
            private NavigableSet<TxnId> waitingOnCommit;
            private NavigableMap<Timestamp, TxnId> waitingOnApply;

            public Update()
            {

            }

            public Update(WaitingOn waitingOn)
            {
                this.waitingOnCommit = waitingOn.waitingOnCommit;
                this.waitingOnApply = waitingOn.waitingOnApply;
            }

            public Update(Committed committed)
            {
                this.waitingOnCommit = committed.waitingOnCommit();
                this.waitingOnApply = committed.waitingOnApply();
            }

            public boolean hasChanges()
            {
                return hasChanges;
            }

            public void addWaitingOnCommit(TxnId txnId)
            {
                waitingOnCommit = ensureSortedMutable(waitingOnCommit);
                waitingOnCommit.add(txnId);
                hasChanges = true;
            }

            public void removeWaitingOnCommit(TxnId txnId)
            {
                if (waitingOnApply == null)
                    return;
                waitingOnCommit = ensureSortedMutable(waitingOnCommit);
                waitingOnCommit.remove(txnId);
                hasChanges = true;
            }

            public void addWaitingOnApply(TxnId txnId, Timestamp executeAt)
            {
                waitingOnApply = ensureSortedMutable(waitingOnApply);
                waitingOnApply.put(executeAt, txnId);
                hasChanges = true;
            }

            public void removeWaitingOnApply(TxnId txnId, Timestamp executeAt)
            {
                if (waitingOnApply == null)
                    return;
                waitingOnApply = ensureSortedMutable(waitingOnApply);
                waitingOnApply.remove(executeAt);
                hasChanges = true;
            }

            public void removeWaitingOn(TxnId txnId, Timestamp executeAt)
            {
                removeWaitingOnCommit(txnId);
                removeWaitingOnApply(txnId, executeAt);
                hasChanges = true;
            }

            public WaitingOn build()
            {
                if ((waitingOnCommit == null || waitingOnCommit.isEmpty()) && (waitingOnApply == null || waitingOnApply.isEmpty()))
                    return EMPTY;
                return new WaitingOn(ensureSortedImmutable(waitingOnCommit), ensureSortedImmutable(waitingOnApply));
            }
        }
    }

    private static Command updateAttributes(Command command, CommonAttributes attributes)
    {
        switch (command.saveStatus())
        {
            case NotWitnessed:
                return NotWitnessed.update((NotWitnessed) command, attributes);
            case PreAccepted:
                return Preaccepted.update((Preaccepted) command, attributes);
            case AcceptedInvalidate:
            case AcceptedInvalidateWithDefinition:
            case Accepted:
            case AcceptedWithDefinition:
                return Accepted.update((Accepted) command, attributes);
            case Committed:
            case ReadyToExecute:
            case Invalidated:
                return Committed.update((Committed) command, attributes);
            case PreApplied:
            case Applied:
                return Executed.update((Executed) command, attributes);
            default:
                throw new IllegalStateException("Unhandled status " + command.status());
        }
    }

    public static class Update implements CommonAttributes
    {
        private boolean completed = false;
        public final SafeCommandStore safeStore;
        private final Command command;

        private RoutingKey homeKey;
        private RoutingKey progressKey;
        private AbstractRoute route;
        private Status.Durability durability;

        private PartialTxn partialTxn;
        private PartialDeps partialDeps;

        private Set<Key> registerKeys = new HashSet<>();
        private Set<CommandListener> listeners;

        public Update(SafeCommandStore safeStore, Command command)
        {
            this.safeStore = safeStore;
            command.checkNotInvalidated();
            this.command = command;
            this.homeKey = command.homeKey();
            this.progressKey = command.progressKey();
            this.route = command.route();
            this.durability = command.durability();
            this.listeners = command.listeners();
            if (command.isWitnessed())
            {
                Preaccepted preaccepted = command.asWitnessed();
                this.partialTxn = preaccepted.partialTxn();
                this.partialDeps = preaccepted.partialDeps();
            }
        }

        private void checkNotCompleted()
        {
            if (completed)
                throw new IllegalStateException(this + " has been completed");
        }

        @Override
        public TxnId txnId()
        {
            return command.txnId();
        }

        public Status status()
        {
            return command.status();
        }

        @Override
        public RoutingKey homeKey()
        {
            checkNotCompleted();
            return homeKey;
        }

        public Update homeKey(RoutingKey homeKey)
        {
            checkNotCompleted();
            this.homeKey = homeKey;
            return this;
        }

        @Override
        public RoutingKey progressKey()
        {
            checkNotCompleted();
            return progressKey;
        }

        public Update progressKey(RoutingKey progressKey)
        {
            checkNotCompleted();
            this.progressKey = progressKey;
            return this;
        }

        @Override
        public AbstractRoute route()
        {
            checkNotCompleted();
            return route;
        }

        public Update route(AbstractRoute route)
        {
            checkNotCompleted();
            this.route = route;
            return this;
        }

        @Override
        public Status.Durability durability()
        {
            checkNotCompleted();
            return Command.durability(durability, command.saveStatus());
        }

        public Update durability(Status.Durability durability)
        {
            checkNotCompleted();
            this.durability = durability;
            return this;
        }

        @Override
        public ImmutableSet<CommandListener> listeners()
        {
            return ensureImmutable(listeners);
        }

        public Update addListener(CommandListener listener)
        {
            listeners = ensureMutable(listeners);
            listeners.add(listener);
            return this;
        }

        public Update removeListener(CommandListener listener)
        {
            if (listener == null || listeners.isEmpty())
                return this;
            listeners = ensureMutable(listeners);
            listeners.remove(listener);
            return this;
        }

        @Override
        public PartialTxn partialTxn()
        {
            checkNotCompleted();
            return partialTxn;
        }

        public Update partialTxn(PartialTxn partialTxn)
        {
            checkNotCompleted();
            this.partialTxn = partialTxn;
            return this;
        }

        @Override
        public PartialDeps partialDeps()
        {
            checkNotCompleted();
            return partialDeps;
        }

        public Update partialDeps(PartialDeps partialDeps)
        {
            checkNotCompleted();
            this.partialDeps = partialDeps;
            return this;
        }

        public Update registerWithKey(Key key)
        {
            checkNotCompleted();
            registerKeys.add(key);
            return this;
        }

        protected  <T extends Command> T complete(T updated)
        {
            checkNotCompleted();

            if (updated == command)
                throw new IllegalStateException("Update is the same as the original");

            if (!registerKeys.isEmpty())
            {
                boolean listenersUpdated = false;
                for (Key key : registerKeys)
                {
                    if (CommandsForKeys.register(safeStore, safeStore.commandsForKey(key), updated))
                    {
                        addListener(CommandsForKey.listener(key));
                        listenersUpdated = true;
                    }
                }
                if (listenersUpdated)
                    updated = (T) Command.updateAttributes(updated, this);

            }

            safeStore.completeUpdate(this, command, updated);
            completed = true;

            if (command != null)
                command.invalidate();

            return updated;
        }

        public Command updateAttributes()
        {
            return complete(Command.updateAttributes(command, this));
        }

        public Preaccepted preaccept(Timestamp executeAt, Ballot ballot)
        {
            if (command.status() == Status.NotWitnessed)
            {
                return complete(Preaccepted.create(this, executeAt, ballot));
            }
            else if (command.status() == Status.AcceptedInvalidate && command.executeAt() == null)
            {
                Accepted accepted = command.asAccepted();
                return Accepted.create(this, accepted.saveStatus(), executeAt, accepted.promised(), accepted.accepted());
            }
            else
            {
                Preconditions.checkState(command.status() == Status.Accepted);
                return (Preaccepted) Command.updateAttributes(command, this);
            }
        }

        public Accepted markDefined()
        {
            Preconditions.checkState(command.hasBeen(Status.Accepted));
            if (isSameClass(command, Accepted.class))
                return Accepted.update(command.asAccepted(), this, SaveStatus.get(command.status(), Definition));
            return (Accepted) Command.updateAttributes(command, this);
        }

        public Preaccepted preacceptInvalidate(Ballot promised)
        {
            switch (command.status())
            {
                case NotWitnessed:
                case PreAccepted:
                    return Preaccepted.create(this, command.executeAt(), promised);
                default:
                    throw new IllegalStateException("Cannot preacceptInvalidate " + command);
            }
        }

        public Accepted accept(Timestamp executeAt, Ballot ballot)
        {
            return complete(new Accepted(this, SaveStatus.Accepted, executeAt, ballot, ballot));
        }

        public Accepted acceptInvalidated(Ballot ballot)
        {
            Timestamp executeAt = command.isWitnessed() ? command.asWitnessed().executeAt() : null;
            return complete(new Accepted(this, SaveStatus.AcceptedInvalidate, executeAt, ballot, ballot));
        }

        public Committed commit(Timestamp executeAt, WaitingOn waitingOn)
        {
            return complete(Committed.create(this, SaveStatus.Committed, executeAt, command.promised(), command.accepted(), waitingOn.waitingOnCommit, waitingOn.waitingOnApply));
        }

        public Committed commitInvalidated()
        {
            return complete(Committed.create(this, SaveStatus.Invalidated, txnId(), command.promised(), command.accepted(), WaitingOn.EMPTY));
        }

        public Committed readyToExecute()
        {
            return complete(Committed.update(command.asCommitted(), this, SaveStatus.ReadyToExecute));
        }

        public Executed preapplied(Timestamp executeAt, WaitingOn waitingOn, Writes writes, Result result)
        {
            return complete(Executed.create(this, SaveStatus.PreApplied, executeAt, command.promised(), command.accepted(), waitingOn, writes, result));
        }

        public Committed noopApplied()
        {
            return complete(Committed.update(command.asCommitted(), this, SaveStatus.Applied));
        }

        public Executed applied()
        {
            return complete(Executed.update(command.asExecuted(), this, SaveStatus.Applied));
        }

    }
}
