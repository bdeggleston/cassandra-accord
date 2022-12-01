package accord.local;

import accord.api.Key;
import accord.api.ProgressLog.ProgressShard;
import accord.api.Result;
import accord.api.RoutingKey;
import accord.local.Command.WaitingOn;
import accord.primitives.*;
import accord.utils.async.AsyncCallbacks;
import accord.utils.async.AsyncChain;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Collections;
import java.util.function.Consumer;
import java.util.function.Function;

import static accord.api.ProgressLog.ProgressShard.*;
import static accord.api.ProgressLog.ProgressShard.Local;
import static accord.local.Commands.EnsureAction.*;
import static accord.local.Status.*;
import static accord.local.Status.ExecutionStatus.Decided;
import static accord.local.Status.ExecutionStatus.Done;
import static accord.local.Status.Known.Nothing;

public class Commands
{
    private static final Logger logger = LoggerFactory.getLogger(Commands.class);

    private Commands()
    {
    }

    private static KeyRanges coordinateRanges(SafeCommandStore safeStore, Command command)
    {
        return safeStore.ranges().at(command.txnId().epoch);
    }

    private static KeyRanges executeRanges(SafeCommandStore safeStore, Timestamp executeAt)
    {
        return safeStore.ranges().since(executeAt.epoch);
    }

    private static KeyRanges covers(@Nullable PartialTxn txn)
    {
        return txn == null ? null : txn.covering();
    }

    private static KeyRanges covers(@Nullable PartialDeps deps)
    {
        return deps == null ? null : deps.covering;
    }

    private static boolean hasQuery(PartialTxn txn)
    {
        return txn != null && txn.query() != null;
    }

    public static class Outcome<T>
    {
        public final Command command;
        public final T status;

        private Outcome(Command command, T status)
        {
            this.command = command;
            this.status = status;
        }

        public static <T> Outcome<T> of(Command command, T status)
        {
            return new Outcome<>(command, status);
        }
    }

    /**
     * true iff this commandStore owns the given key on the given epoch
     */
    public static boolean owns(SafeCommandStore safeStore, long epoch, RoutingKey someKey)
    {
        if (!safeStore.commandStore().hashIntersects(someKey))
            return false;

        return safeStore.ranges().at(epoch).contains(someKey);
    }

    enum EnsureAction
    {Ignore, Check, Add, TrySet, Set}

    private static ProgressShard progressShard(SafeCommandStore safeStore, Command command)
    {
        RoutingKey progressKey = command.progressKey();
        if (progressKey == null)
            return Unsure;

        if (progressKey == noProgressKey())
            return No;

        KeyRanges coordinateRanges = safeStore.ranges().at(command.txnId().epoch);
        if (!coordinateRanges.contains(progressKey))
            return No;

        if (!safeStore.commandStore().hashIntersects(progressKey))
            return No;

        return progressKey.equals(command.homeKey()) ? Home : Local;
    }


    private static ProgressShard progressShard(SafeCommandStore safeStore, Command.Update update, AbstractRoute route, KeyRanges coordinateRanges)
    {
        if (update.progressKey() == null)
            return Unsure;

        return progressShard(safeStore, update, route, update.progressKey(), coordinateRanges);
    }

    private static ProgressShard progressShard(SafeCommandStore safeStore, Command.Update update, AbstractRoute route, @Nullable RoutingKey progressKey, KeyRanges coordinateRanges)
    {
        updateHomeKey(safeStore, update, route.homeKey);

        if (progressKey == null || progressKey == NO_PROGRESS_KEY)
        {
            if (update.progressKey() == null)
                update.progressKey(NO_PROGRESS_KEY);

            return No;
        }

        if (update.progressKey() == null) update.progressKey(progressKey);
        else if (!update.progressKey().equals(progressKey)) throw new AssertionError();

        if (!coordinateRanges.contains(progressKey))
            return No;

        if (!safeStore.commandStore().hashIntersects(progressKey))
            return No;

        return progressKey.equals(update.homeKey()) ? Home : Local;
    }

    private static boolean validateRoute(AbstractRoute currentRoute, KeyRanges existingRanges, KeyRanges additionalRanges,
                                         ProgressShard shard, AbstractRoute route, EnsureAction ensureRoute)
    {
        if (shard.isProgress())
        {
            // validate route
            if (shard.isHome())
            {
                switch (ensureRoute)
                {
                    default:
                        throw new AssertionError();
                    case Check:
                        if (!(currentRoute instanceof Route) && !(route instanceof Route))
                            return false;
                    case Ignore:
                        break;
                    case Add:
                    case Set:
                        if (!(route instanceof Route))
                            throw new IllegalArgumentException("Incomplete route (" + route + ") sent to home shard");
                        break;
                    case TrySet:
                        if (!(route instanceof Route))
                            return false;
                }
            }
            else if (currentRoute == null)
            {
                // failing any of these tests is always an illegal state
                if (!route.covers(existingRanges))
                    return false;

                if (existingRanges != additionalRanges && !route.covers(additionalRanges))
                    throw new IllegalArgumentException("Incomplete route (" + route + ") provided; does not cover " + additionalRanges);
            }
            else if (existingRanges != additionalRanges && !currentRoute.covers(additionalRanges))
            {
                if (!route.covers(additionalRanges))
                    throw new IllegalArgumentException("Incomplete route (" + route + ") provided; does not cover " + additionalRanges);
            }
            else
            {
                if (!currentRoute.covers(existingRanges))
                    throw new IllegalStateException();
            }
        }
        return true;
    }

    /**
     * Validate we have sufficient information for the route, partialTxn and partialDeps fields, and if so update them;
     * otherwise return false (or throw an exception if an illegal state is encountered)
     */
    private static boolean validate(Command update, KeyRanges existingRanges, KeyRanges additionalRanges, ProgressShard shard,
                                    AbstractRoute route, EnsureAction ensureRoute,
                                    @Nullable PartialTxn partialTxn, EnsureAction ensurePartialTxn,
                                    @Nullable PartialDeps partialDeps, EnsureAction ensurePartialDeps)
    {
        if (shard == Unsure)
            return false;

        // first validate route
        if (update.isWitnessed() && !validateRoute(update.asWitnessed().route(), existingRanges, additionalRanges, shard, route, ensureRoute))
            return false;

        // invalid to Add deps to Accepted or AcceptedInvalidate statuses, as Committed deps are not equivalent
        // and we may erroneously believe we have covered a wider range than we have infact covered
        if (ensurePartialDeps == Add)
            Preconditions.checkState(update.status() != Accepted && update.status() != AcceptedInvalidate);

        PartialTxn currentTxn = update.isWitnessed() ? update.asWitnessed().partialTxn() : null;

        // validate new partial txn
        if (!validate(ensurePartialTxn, existingRanges, additionalRanges, covers(currentTxn), covers(partialTxn), "txn", partialTxn))
            return false;

        if (shard.isHome() && ensurePartialTxn != Ignore)
        {
            if (!hasQuery(currentTxn) && !hasQuery(partialTxn))
                throw new IllegalStateException();
        }

        PartialDeps currentDeps = update.isWitnessed() ? update.asWitnessed().partialDeps() : null;

        return validate(ensurePartialDeps, existingRanges, additionalRanges, covers(currentDeps), covers(partialDeps), "deps", partialDeps);
    }

    /**
     * Validate we have sufficient information for the route, partialTxn and partialDeps fields, and if so update them;
     * otherwise return false (or throw an exception if an illegal state is encountered)
     */
    private static boolean validate(Command.Update update, KeyRanges existingRanges, KeyRanges additionalRanges, ProgressShard shard,
                                    AbstractRoute route, EnsureAction ensureRoute,
                                    @Nullable PartialTxn partialTxn, EnsureAction ensurePartialTxn,
                                    @Nullable PartialDeps partialDeps, EnsureAction ensurePartialDeps)
    {
        if (shard == Unsure)
            return false;

        // first validate route
        if (!validateRoute(update.route(), existingRanges, additionalRanges, shard, route, ensureRoute))
            return false;

        // invalid to Add deps to Accepted or AcceptedInvalidate statuses, as Committed deps are not equivalent
        // and we may erroneously believe we have covered a wider range than we have infact covered
        if (ensurePartialDeps == Add)
            Preconditions.checkState(update.status() != Accepted && update.status() != AcceptedInvalidate);

        // validate new partial txn
        if (!validate(ensurePartialTxn, existingRanges, additionalRanges, covers(update.partialTxn()), covers(partialTxn), "txn", partialTxn))
            return false;

        if (shard.isHome() && ensurePartialTxn != Ignore)
        {
            if (!hasQuery(update.partialTxn()) && !hasQuery(partialTxn))
                throw new IllegalStateException();
        }

        return validate(ensurePartialDeps, existingRanges, additionalRanges, covers(update.partialDeps()), covers(partialDeps), "deps", partialDeps);
    }

    // TODO: rename
    private static void set(SafeCommandStore safeStore, Command.Update update,
                            KeyRanges existingRanges, KeyRanges additionalRanges, ProgressShard shard, AbstractRoute route,
                            @Nullable PartialTxn partialTxn, EnsureAction ensurePartialTxn,
                            @Nullable PartialDeps partialDeps, EnsureAction ensurePartialDeps)
    {
        Preconditions.checkState(update.progressKey() != null);
        KeyRanges allRanges = existingRanges.union(additionalRanges);

        if (shard.isProgress()) update.route(AbstractRoute.merge(update.route(), route));
        else update.route(AbstractRoute.merge(update.route(), route.slice(allRanges)));

        // TODO (soon): stop round-robin hashing; partition only on ranges
        switch (ensurePartialTxn)
        {
            case Add:
                if (partialTxn == null)
                    break;

                if (update.partialTxn() != null)
                {
                    partialTxn = partialTxn.slice(allRanges, shard.isHome());
                    partialTxn.keys().foldlDifference(update.partialTxn().keys(), (i, key, p, v) -> {
                        if (safeStore.commandStore().hashIntersects(key))
                            update.registerWithKey(key);
                        return v;
                    }, 0, 0, 1);
                    update.partialTxn(update.partialTxn().with(partialTxn));
                    break;
                }

            case Set:
            case TrySet:
                update.partialTxn(partialTxn = partialTxn.slice(allRanges, shard.isHome()));
                partialTxn.keys().forEach(key -> {
                    if (safeStore.commandStore().hashIntersects(key))
                        update.registerWithKey(key);
                });
                break;
        }

        switch (ensurePartialDeps)
        {
            case Add:
                if (partialDeps == null)
                    break;

                if (update.partialDeps() != null)
                {
                    update.partialDeps(update.partialDeps().with(partialDeps.slice(allRanges)));
                    break;
                }

            case Set:
            case TrySet:
                update.partialDeps(partialDeps.slice(allRanges));
                break;
        }
    }

    private static boolean validate(EnsureAction action, KeyRanges existingRanges, KeyRanges additionalRanges,
                                    KeyRanges existing, KeyRanges adding, String kind, Object obj)
    {
        switch (action)
        {
            default:
                throw new IllegalStateException();
            case Ignore:
                break;

            case TrySet:
                if (adding != null)
                {
                    if (!adding.contains(existingRanges))
                        return false;

                    if (additionalRanges != existingRanges && !adding.contains(additionalRanges))
                        return false;

                    break;
                }
            case Set:
                // failing any of these tests is always an illegal state
                Preconditions.checkState(adding != null);
                if (!adding.contains(existingRanges))
                    throw new IllegalArgumentException("Incomplete " + kind + " (" + obj + ") provided; does not cover " + existingRanges);

                if (additionalRanges != existingRanges && !adding.contains(additionalRanges))
                    throw new IllegalArgumentException("Incomplete " + kind + " (" + obj + ") provided; does not cover " + additionalRanges);
                break;

            case Check:
            case Add:
                if (adding == null)
                {
                    if (existing == null)
                        return false;

                    Preconditions.checkState(existing.contains(existingRanges));
                    if (existingRanges != additionalRanges && !existing.contains(additionalRanges))
                    {
                        if (action == Check)
                            return false;

                        throw new IllegalArgumentException("Missing additional " + kind + "; existing does not cover " + additionalRanges.difference(existingRanges));
                    }
                }
                else if (existing != null)
                {
                    KeyRanges covering = adding.union(existing);
                    Preconditions.checkState(covering.contains(existingRanges));
                    if (existingRanges != additionalRanges && !covering.contains(additionalRanges))
                    {
                        if (action == Check)
                            return false;

                        throw new IllegalArgumentException("Incomplete additional " + kind + " (" + obj + ") provided; does not cover " + additionalRanges.difference(existingRanges));
                    }
                }
                else
                {
                    if (!adding.contains(existingRanges))
                        return false;

                    if (existingRanges != additionalRanges && !adding.contains(additionalRanges))
                    {
                        if (action == Check)
                            return false;

                        throw new IllegalArgumentException("Incomplete additional " + kind + " (" + obj + ") provided; does not cover " + additionalRanges.difference(existingRanges));
                    }
                }
                break;
        }

        return true;
    }

    // TODO: this is an ugly hack, need to encode progress/homeKey/Route state combinations much more clearly
    //  (perhaps introduce encapsulating class representing each possible arrangement)
    private static final RoutingKey NO_PROGRESS_KEY = new RoutingKey()
    {
        @Override
        public int routingHash()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public int compareTo(@Nonnull RoutingKey ignore)
        {
            throw new UnsupportedOperationException();
        }
    };

    public static RoutingKey noProgressKey()
    {
        return NO_PROGRESS_KEY;
    }

    public enum AcceptOutcome {Success, Redundant, RejectedBallot}

    public static Outcome<AcceptOutcome> preaccept(SafeCommandStore safeStore, TxnId txnId, PartialTxn partialTxn, AbstractRoute route, @Nullable RoutingKey progressKey)
    {
        Command command = safeStore.command(txnId);
        if (command.promised().compareTo(Ballot.ZERO) > 0)
            return Outcome.of(command, AcceptOutcome.RejectedBallot);

        return preacceptInternal(safeStore, command, partialTxn, route, progressKey, Ballot.ZERO);
    }

    private static Outcome<AcceptOutcome> preacceptInternal(SafeCommandStore safeStore, Command command, PartialTxn partialTxn, AbstractRoute route, @Nullable RoutingKey progressKey, Ballot ballot)
    {
        if (command.known() != Nothing)
        {
            Preconditions.checkState(command.status() == Invalidated || !command.isWitnessed() || command.asWitnessed().executeAt() != null);
            logger.trace("{}: skipping preaccept - already preaccepted ({})", command.txnId(), command.status());
            return Outcome.of(command, AcceptOutcome.Redundant);
        }

        TxnId txnId = command.txnId();

        KeyRanges coordinateRanges = coordinateRanges(safeStore, command);
        Command.Update update = safeStore.beginUpdate(command);
        ProgressShard shard = progressShard(safeStore, update, route, progressKey, coordinateRanges);
        if (!validate(command, KeyRanges.EMPTY, coordinateRanges, shard, route, Set, partialTxn, Set, null, Ignore))
            throw new IllegalStateException();

        // FIXME: this should go into a consumer method
        set(safeStore, update, KeyRanges.EMPTY, coordinateRanges, shard, route, partialTxn, Set, null, Ignore);
        if (command.executeAt() == null)
        {
            // unlike in the Accord paper, we partition shards within a node, so that to ensure a total order we must either:
            //  - use a global logical clock to issue new timestamps; or
            //  - assign each shard _and_ process a unique id, and use both as components of the timestamp
            command = update.preaccept(safeStore.preaccept(txnId, partialTxn.keys()), ballot);
            safeStore.progressLog().preaccepted(command, shard);
        }
        else
        {
            command = update.markDefined();
        }

        safeStore.notifyListeners(command);
        return Outcome.of(command, AcceptOutcome.Success);
    }

    public static Outcome<Boolean> preacceptInvalidate(SafeCommandStore safeStore, TxnId txnId, Ballot ballot)
    {
        Command command = safeStore.command(txnId);
        if (command.promised().compareTo(ballot) > 0)
        {
            logger.trace("{}: skipping preacceptInvalidate - witnessed higher ballot ({})", command.txnId(), command.promised());
            return Outcome.of(command, false);
        }
        Command.Update update = safeStore.beginUpdate(command);
        command = update.preacceptInvalidate(ballot);
        return Outcome.of(command, true);
    }

    public static Outcome<AcceptOutcome> accept(SafeCommandStore safeStore, TxnId txnId, Ballot ballot, PartialRoute route, Keys keys, @Nullable RoutingKey progressKey, Timestamp executeAt, PartialDeps partialDeps)
    {
        Command command = safeStore.command(txnId);
        if (command.promised().compareTo(ballot) > 0)
        {
            logger.trace("{}: skipping accept - witnessed higher ballot ({} > {})", command.txnId(), command.promised(), ballot);
            return Outcome.of(command, AcceptOutcome.RejectedBallot);
        }

        if (command.hasBeen(Committed))
        {
            logger.trace("{}: skipping accept - already committed ({})", command.txnId(), command.status());
            return Outcome.of(command, AcceptOutcome.Redundant);
        }

        KeyRanges coordinateRanges = coordinateRanges(safeStore, command);
        KeyRanges executeRanges = txnId.epoch == executeAt.epoch ? coordinateRanges : safeStore.ranges().at(executeAt.epoch);
        KeyRanges acceptRanges = txnId.epoch == executeAt.epoch ? coordinateRanges : safeStore.ranges().between(txnId.epoch, executeAt.epoch);

        Command.Update update = safeStore.beginUpdate(command);
        ProgressShard shard = progressShard(safeStore, update, route, progressKey, coordinateRanges);

        if (!validate(update, coordinateRanges, executeRanges, shard, route, Ignore, null, Ignore, partialDeps, Set))
            throw new AssertionError("Invalid response from validate function");

        set(safeStore, update, coordinateRanges, executeRanges, shard, route, null, Ignore, partialDeps, Set);
        switch (command.status())
        {
            // if we haven't already registered, do so, to correctly maintain max per-key timestamp
            case NotWitnessed:
            case AcceptedInvalidate:
                keys.foldl(acceptRanges, (i, k, p, v) -> {
                    if (safeStore.commandStore().hashIntersects(k))
                        update.registerWithKey(k);
                    return 0L;
                }, 0L, 0L, 1L);
        }

        command = update.accept(executeAt, ballot);
        safeStore.progressLog().accepted(command, shard);
        safeStore.notifyListeners(command);

        return Outcome.of(command, AcceptOutcome.Success);
    }

    public static Outcome<AcceptOutcome> acceptInvalidate(SafeCommandStore safeStore, TxnId txnId, Ballot ballot)
    {
        Command command = safeStore.command(txnId);
        if (command.promised().compareTo(ballot) > 0)
        {
            logger.trace("{}: skipping accept invalidated - witnessed higher ballot ({} > {})", command.txnId(), command.promised(), ballot);
            return Outcome.of(command, AcceptOutcome.RejectedBallot);
        }

        if (command.hasBeen(Committed))
        {
            logger.trace("{}: skipping accept invalidated - already committed ({})", command.txnId(), command.status());
            return Outcome.of(command, AcceptOutcome.Redundant);
        }

        Command.Update update = safeStore.beginUpdate(command);
        logger.trace("{}: accepted invalidated", update.txnId());

        command = update.acceptInvalidated(ballot);
        safeStore.notifyListeners(command);
        return Outcome.of(command, AcceptOutcome.Success);
    }

    public enum CommitOutcome {Success, Redundant, Insufficient;}

    // relies on mutual exclusion for each key
    public static Outcome<CommitOutcome> commit(SafeCommandStore safeStore, TxnId txnId, AbstractRoute route, @Nullable RoutingKey progressKey, @Nullable PartialTxn partialTxn, Timestamp executeAt, PartialDeps partialDeps)
    {
        Command command = safeStore.command(txnId);
        if (command.hasBeen(Committed))
        {
            logger.trace("{}: skipping commit - already committed ({})", command.txnId(), command.status());
            if (executeAt.equals(command.executeAt()) && command.status() != Invalidated)
                return Outcome.of(command, CommitOutcome.Redundant);

            safeStore.agent().onInconsistentTimestamp(command, (command.status() == Invalidated ? Timestamp.NONE : command.executeAt()), executeAt);
            throw new AssertionError("Inconsistent execution timestamp detected for txnId " + command.txnId() + ": " + command.executeAt() + " != " + executeAt);
        }

        KeyRanges coordinateRanges = coordinateRanges(safeStore, command);
        // TODO (now): consider ranges between coordinateRanges and executeRanges? Perhaps don't need them
        KeyRanges executeRanges = executeRanges(safeStore, executeAt);

        Command.Update update = safeStore.beginUpdate(command);
        ProgressShard shard = progressShard(safeStore, update, route, progressKey, coordinateRanges);

        if (!validate(update, coordinateRanges, executeRanges, shard, route, Check, partialTxn, Add, partialDeps, Set))
            return Outcome.of(update.updateAttributes(), CommitOutcome.Insufficient);

        // FIXME: split up set
        set(safeStore, update, coordinateRanges, executeRanges, shard, route, partialTxn, Add, partialDeps, Set);

        logger.trace("{}: committed with executeAt: {}, deps: {}", update.txnId(), executeAt, partialDeps);
        WaitingOn waitingOn = populateWaitingOn(safeStore, txnId, executeAt, partialDeps);

        command = update.commit(executeAt, waitingOn);
        safeStore.progressLog().committed(command, shard);

        // TODO (now): introduce intermediate status to avoid reentry when notifying listeners (which might notify us)
        command = maybeExecute(safeStore, command, shard, true, true);
        return Outcome.of(command, CommitOutcome.Success);
    }

    protected static WaitingOn populateWaitingOn(SafeCommandStore safeStore, TxnId txnId, Timestamp executeAt, PartialDeps partialDeps)
    {
        KeyRanges ranges = safeStore.ranges().since(executeAt.epoch);
        if (ranges == null)
            return WaitingOn.EMPTY;

        WaitingOn.Update update = new WaitingOn.Update();
        partialDeps.forEachOn(ranges, safeStore.commandStore()::hashIntersects, depId -> {
            Command command = safeStore.ifLoaded(depId);
            if (command == null)
            {
                update.addWaitingOnCommit(depId);
                safeStore.addAndInvokeListener(depId, txnId);
            }
            else
            {
                switch (command.status())
                {
                    default:
                        throw new IllegalStateException();
                    case NotWitnessed:
                    case PreAccepted:
                    case Accepted:
                    case AcceptedInvalidate:
                        // we don't know when these dependencies will execute, and cannot execute until we do

                        Command.addListener(safeStore, command, Command.listener(txnId));
                        update.addWaitingOnCommit(command.txnId());
                        break;
                    case Committed:
                        // TODO: split into ReadyToRead and ReadyToWrite;
                        //       the distributed read can be performed as soon as those keys are ready, and in parallel with any other reads
                        //       the client can even ACK immediately after; only the write needs to be postponed until other in-progress reads complete
                    case ReadyToExecute:
                    case PreApplied:
                    case Applied:
                        Command.addListener(safeStore, command, Command.listener(txnId));
                        insertPredecessor(txnId, executeAt, update, command);
                    case Invalidated:
                        break;
                }
            }
        });
        return update.build();
    }

    // TODO (now): commitInvalidate may need to update cfks _if_ possible
    public static void commitInvalidate(SafeCommandStore safeStore, TxnId txnId)
    {
        Command command = safeStore.command(txnId);
        if (command.hasBeen(Committed))
        {
            logger.trace("{}: skipping commit invalidated - already committed ({})", command.txnId(), command.status());
            if (!command.hasBeen(Invalidated))
                safeStore.agent().onInconsistentTimestamp(command, Timestamp.NONE, command.executeAt());

            return;
        }

        ProgressShard shard = progressShard(safeStore, command);
        safeStore.progressLog().invalidated(command, shard);

        Command.Update update = safeStore.beginUpdate(command);
        if (update.partialDeps() == null)
            update.partialDeps(PartialDeps.NONE);
        command = update.commitInvalidated();
        logger.trace("{}: committed invalidated", txnId);

        safeStore.notifyListeners(command);
    }

    public enum ApplyOutcome {Success, Redundant, Insufficient}

    public static Outcome<ApplyOutcome> apply(SafeCommandStore safeStore, TxnId txnId, long untilEpoch, AbstractRoute route, Timestamp executeAt, @Nullable PartialDeps partialDeps, Writes writes, Result result)
    {
        Command command = safeStore.command(txnId);
        if (command.hasBeen(PreApplied) && executeAt.equals(command.executeAt()))
        {
            logger.trace("{}: skipping apply - already executed ({})", command.txnId(), command.status());
            return Outcome.of(command, ApplyOutcome.Redundant);
        }
        else if (command.hasBeen(Committed) && !executeAt.equals(command.executeAt()))
        {
            safeStore.agent().onInconsistentTimestamp(command, command.executeAt(), executeAt);
        }

        KeyRanges coordinateRanges = coordinateRanges(safeStore, command);
        KeyRanges executeRanges = executeRanges(safeStore, executeAt);
        if (untilEpoch < safeStore.latestEpoch())
        {
            KeyRanges expectedRanges = safeStore.ranges().between(executeAt.epoch, untilEpoch);
            Preconditions.checkState(expectedRanges.contains(executeRanges));
        }

        Command.Update update = safeStore.beginUpdate(command);
        ProgressShard shard = progressShard(safeStore, update, route, coordinateRanges);

        if (!validate(update, coordinateRanges, executeRanges, shard, route, Check, null, Check, partialDeps, command.hasBeen(Committed) ? Add : TrySet))
            return Outcome.of(update.updateAttributes(), ApplyOutcome.Insufficient); // TODO: this should probably be an assertion failure if !TrySet

        set(safeStore, update, coordinateRanges, executeRanges, shard, route, null, Check, partialDeps, command.hasBeen(Committed) ? Add : TrySet);

        WaitingOn waitingOn = !command.hasBeen(Committed) ? populateWaitingOn(safeStore, txnId, executeAt, partialDeps) : WaitingOn.EMPTY;
        command = update.preapplied(executeAt, waitingOn, writes, result);
        logger.trace("{}: apply, status set to Executed with executeAt: {}, deps: {}", update.txnId(), executeAt, partialDeps);

        command = maybeExecute(safeStore, command, shard, true, true);
        safeStore.progressLog().executed(command, shard);
        return Outcome.of(command, ApplyOutcome.Success);
    }

    protected static void postApply(SafeCommandStore safeStore, TxnId txnId)
    {
        Command command = safeStore.command(txnId);
        logger.trace("{} applied, setting status to Applied and notifying listeners", command.txnId());
        command = safeStore.beginUpdate(command).applied();
        safeStore.notifyListeners(command);
    }

    private static Function<SafeCommandStore, Void> callPostApply(TxnId txnId)
    {
        return safeStore -> {
            postApply(safeStore, txnId);
            return null;
        };
    }

    protected static AsyncChain<Void> applyChain(SafeCommandStore safeStore, Command.Executed command)
    {
        // important: we can't include a reference to *this* in the lambda, since the C* implementation may evict
        // the command instance from memory between now and the write completing (and post apply being called)
        CommandStore unsafeStore = safeStore.commandStore();
        TxnId txnId = command.txnId();
        PreLoadContext context = command.contextForSelf();
        return command.writes().apply(safeStore).flatMap(unused -> unsafeStore.submit(context, callPostApply(txnId)));
    }

    private static void apply(SafeCommandStore safeStore, Command.Executed command)
    {
        applyChain(safeStore, command).begin(AsyncCallbacks.throwOnFailure());
    }

    // TODO: maybe split into maybeExecute and maybeApply?
    public static Command maybeExecute(SafeCommandStore safeStore, Command command, ProgressShard shard, boolean alwaysNotifyListeners, boolean notifyWaitingOn)
    {
        if (logger.isTraceEnabled())
            logger.trace("{}: Maybe executing with status {}. Will notify listeners on noop: {}", command.txnId(), command.status(), alwaysNotifyListeners);

        if (command.status() != Committed && command.status() != PreApplied)
        {
            if (alwaysNotifyListeners)
                safeStore.notifyListeners(command);
            return command;
        }

        if (command.asCommitted().isUnableToExecute())
        {
            if (alwaysNotifyListeners)
                safeStore.notifyListeners(command);

            if (notifyWaitingOn)
                new NotifyWaitingOn(command.txnId()).accept(safeStore);
            return command;
        }

        switch (command.status())
        {
            case Committed:
                // TODO: maintain distinct ReadyToRead and ReadyToWrite states
                command = safeStore.beginUpdate(command).readyToExecute();
                logger.trace("{}: set to ReadyToExecute", command.txnId());
                safeStore.progressLog().readyToExecute(command, shard);
                safeStore.notifyListeners(command);
                return command;

            case PreApplied:
                Command.Executed executed = command.asExecuted();
                if (executeRanges(safeStore, executed.executeAt()).intersects(executed.writes().keys, safeStore.commandStore()::hashIntersects))
                {
                    logger.trace("{}: applying", command.txnId());
                    apply(safeStore, executed);
                    return executed;
                }
                else
                {
                    logger.trace("{}: applying no-op", command.txnId());
                    command = safeStore.beginUpdate(command).noopApplied();
                    safeStore.notifyListeners(command);
                    return command;
                }
            default:
                throw new IllegalStateException();
        }
    }

    public static boolean maybeExecute(SafeCommandStore safeStore, Command command, boolean alwaysNotifyListeners, boolean notifyWaitingOn)
    {
        Command updated = maybeExecute(safeStore, command, progressShard(safeStore, command), alwaysNotifyListeners, notifyWaitingOn);
        return updated != command;
    }

    /**
     * @param dependency is either committed or invalidated
     * @return true iff {@code maybeExecute} might now have a different outcome
     */
    static boolean updatePredecessor(SafeCommandStore safeStore, Command.Committed command, WaitingOn.Update waitingOn, Command dependency)
    {
        Preconditions.checkState(dependency.hasBeen(Committed));
        if (dependency.hasBeen(Invalidated))
        {
            logger.trace("{}: {} is invalidated. Stop listening and removing from waiting on commit set.", command.txnId(), dependency.txnId());

            Command.removeListener(safeStore, dependency, command.asListener());
            waitingOn.removeWaitingOnCommit(dependency.txnId()); // TODO (now): this was missing in partial-replication; might be redundant?
            return true;
        }
        else if (dependency.executeAt().compareTo(command.executeAt()) > 0)
        {
            // cannot be a predecessor if we execute later
            logger.trace("{}: {} executes after us. Stop listening and removing from waiting on apply set.", command.txnId(), dependency.txnId());
            waitingOn.removeWaitingOn(dependency.txnId(), dependency.executeAt());

            Command.removeListener(safeStore, dependency, command.asListener());
            return true;
        }
        else if (dependency.hasBeen(Applied))
        {
            logger.trace("{}: {} has been applied. Stop listening and removing from waiting on apply set.", command.txnId(), dependency.txnId());
            waitingOn.removeWaitingOn(dependency.txnId(), dependency.executeAt());

            Command.removeListener(safeStore, dependency, command.asListener());
            return true;
        }
        else if (command.isUnableToExecute())
        {
            logger.trace("{}: adding {} to waiting on apply set.", command.txnId(), dependency.txnId());
            waitingOn.addWaitingOnApply(dependency.txnId(), dependency.executeAt());
            waitingOn.removeWaitingOnCommit(dependency.txnId());
            return false;
        }
        else
        {
            throw new IllegalStateException();
        }
    }

    private static void insertPredecessor(TxnId txnId, Timestamp executeAt, WaitingOn.Update update, Command dependency)
    {
        Preconditions.checkState(dependency.hasBeen(Committed));
        if (dependency.hasBeen(Invalidated))
        {
            logger.trace("{}: {} is invalidated. Do not insert.", txnId, dependency.txnId());
        }
        else if (dependency.executeAt().compareTo(executeAt) > 0)
        {
            // dependency cannot be a predecessor if it executes later
            logger.trace("{}: {} executes after us. Do not insert.", txnId, dependency.txnId());
        }
        else if (dependency.hasBeen(Applied))
        {
            logger.trace("{}: {} has been applied. Do not insert.", txnId, dependency.txnId());
        }
        else
        {
            logger.trace("{}: adding {} to waiting on apply set.", txnId, dependency.txnId());
            update.addWaitingOnApply(dependency.txnId(), dependency.executeAt());
        }
    }

    static void updatePredecessorAndMaybeExecute(SafeCommandStore safeStore, Command.Committed command, Command predecessor, boolean notifyWaitingOn)
    {
        if (command.hasBeen(Applied))
            return;

        WaitingOn.Update waitingOn = new WaitingOn.Update(command);
        boolean attemptExecution = updatePredecessor(safeStore, command, waitingOn, predecessor);
        command = Command.updateWaitingOn(safeStore, command, waitingOn);

        if (attemptExecution)
            maybeExecute(safeStore, command, progressShard(safeStore, command), false, notifyWaitingOn);
    }

    public static Outcome<AcceptOutcome> recover(SafeCommandStore safeStore, TxnId txnId, PartialTxn partialTxn, AbstractRoute route, @Nullable RoutingKey progressKey, Ballot ballot)
    {
        Command command = safeStore.command(txnId);
        if (command.promised().compareTo(ballot) > 0)
        {
            logger.trace("{}: skipping preaccept invalidate - higher ballot witnessed ({})", command.txnId(), command.promised());
            return Outcome.of(command, AcceptOutcome.RejectedBallot);
        }

        if (command.executeAt() == null)
        {
            Preconditions.checkState(command.status() == NotWitnessed || command.status() == AcceptedInvalidate);
            Outcome<AcceptOutcome> outcome = preacceptInternal(safeStore, command, partialTxn, route, progressKey, ballot);
            switch (outcome.status)
            {
                default:
                case RejectedBallot:
                case Redundant:
                    throw new IllegalStateException();

                case Success:
            }
            return outcome;
        }

        return Outcome.of(command, AcceptOutcome.Success);
    }

    private static Command setDurability(SafeCommandStore safeStore, Command command, Durability durability, RoutingKey homeKey, @Nullable Timestamp executeAt)
    {
        Command.Update update = safeStore.beginUpdate(command);
        updateHomeKey(safeStore, update, homeKey);
        if (executeAt != null && update.status().hasBeen(Committed) && !command.asCommitted().executeAt().equals(executeAt))
            safeStore.agent().onInconsistentTimestamp(command, command.asCommitted().executeAt(), executeAt);
        update.durability(durability);
        return update.updateAttributes();
    }

    public static Command setDurability(SafeCommandStore safeStore, TxnId txnId, Durability durability, RoutingKey homeKey, @Nullable Timestamp executeAt)
    {
        Command command = safeStore.command(txnId);
        return setDurability(safeStore, command, durability, homeKey, executeAt);
    }

    public static Command updateHomeKey(SafeCommandStore safeStore, Command command, RoutingKey homeKey)
    {
        if (command.homeKey() == null)
        {
            Command.Update update = safeStore.beginUpdate(command);
            update.homeKey(homeKey);
            if (update.progressKey() == null && owns(safeStore, update.txnId().epoch, homeKey))
                update.progressKey(homeKey);

            return update.updateAttributes();
        }
        else if (!command.homeKey().equals(homeKey))
        {
            throw new IllegalStateException();
        }

        return command;
    }

    public static void updateHomeKey(SafeCommandStore safeStore, Command.Update update, RoutingKey homeKey)
    {
        if (update.homeKey() == null)
        {
            update.homeKey(homeKey);
            if (update.progressKey() == null && safeStore.owns(update.txnId().epoch, homeKey))
                update.progressKey(homeKey);
        }
        else if (!update.homeKey().equals(homeKey))
        {
            throw new IllegalStateException();
        }
    }

    public static void saveRoute(SafeCommandStore safeStore, TxnId txnId, Route route)
    {
        Command.Update update = safeStore.beginUpdate(txnId);
        update.route(route);
        updateHomeKey(safeStore, update, route.homeKey);
        update.updateAttributes();
    }

    public static void listenerUpdate(SafeCommandStore safeStore, Command listener, Command command)
    {
        switch (command.status())
        {
            default:
                throw new IllegalStateException();
            case NotWitnessed:
            case PreAccepted:
            case Accepted:
            case AcceptedInvalidate:
                break;

            case Committed:
            case ReadyToExecute:
            case PreApplied:
            case Applied:
            case Invalidated:
                updatePredecessorAndMaybeExecute(safeStore, listener.asCommitted(), command, false);
                break;
        }
    }

    static class NotifyWaitingOn implements PreLoadContext, Consumer<SafeCommandStore>
    {
        ExecutionStatus[] blockedUntil = new ExecutionStatus[4];
        TxnId[] txnIds = new TxnId[4];
        int depth;

        public NotifyWaitingOn(TxnId txnId)
        {
            txnIds[0] = txnId;
            blockedUntil[0] = Done;
        }

        @Override
        public void accept(SafeCommandStore safeStore)
        {
            Command prev = get(safeStore, depth - 1);
            while (depth >= 0)
            {
                Command cur = safeStore.ifLoaded(txnIds[depth]);
                ExecutionStatus until = blockedUntil[depth];
                if (cur == null)
                {
                    // need to load; schedule execution for later
                    safeStore.commandStore().execute(this, this).begin(AsyncCallbacks.throwOnFailure());
                    return;
                }

                if (prev != null)
                {
                    if (cur.hasBeen(until) || (cur.hasBeen(Committed) && cur.executeAt().compareTo(prev.executeAt()) > 0))
                    {
                        updatePredecessorAndMaybeExecute(safeStore, prev.asCommitted(), cur, false);
                        --depth;
                        prev = get(safeStore, depth - 1);
                        continue;
                    }
                }
                else if (cur.hasBeen(until))
                {
                    // we're done; have already applied
                    Preconditions.checkState(depth == 0);
                    break;
                }

                TxnId directlyBlockedBy = cur.isCommitted() ? cur.asCommitted().firstWaitingOnCommit() : null;
                if (directlyBlockedBy != null)
                {
                    push(directlyBlockedBy, Decided);
                }
                else if (null != (directlyBlockedBy = cur.isCommitted() ? cur.asCommitted().firstWaitingOnApply() : null))
                {
                    push(directlyBlockedBy, Done);
                }
                else
                {
                    if (cur.hasBeen(Committed) && !cur.hasBeen(ReadyToExecute) && !cur.asCommitted().isUnableToExecute())
                    {
                        if (!maybeExecute(safeStore, cur, false, false))
                            throw new AssertionError("Is able to Apply, but has not done so");
                        // loop and re-test the command's status; we may still want to notify blocking, esp. if not homeShard
                        continue;
                    }

                    RoutingKeys someKeys = cur.maxRoutingKeys();
                    if (someKeys == null && prev != null) someKeys = prev.partialDeps().someRoutingKeys(cur.txnId());
                    Preconditions.checkState(someKeys != null);
                    logger.trace("{} blocked on {} until {}", txnIds[0], cur.txnId(), until);
                    safeStore.progressLog().waiting(cur.txnId(), until.requires, someKeys);
                    return;
                }
                prev = cur;
            }
        }

        private Command get(SafeCommandStore safeStore, int i)
        {
            return i >= 0 ? safeStore.command(txnIds[i]) : null;
        }

        void push(TxnId by, ExecutionStatus until)
        {
            if (++depth == txnIds.length)
            {
                txnIds = Arrays.copyOf(txnIds, txnIds.length * 2);
                blockedUntil = Arrays.copyOf(blockedUntil, txnIds.length);
            }
            txnIds[depth] = by;
            blockedUntil[depth] = until;
        }

        @Override
        public Iterable<TxnId> txnIds()
        {
            return Arrays.asList(txnIds).subList(0, depth + 1);
        }

        @Override
        public Iterable<Key> keys()
        {
            return Collections.emptyList();
        }

    }
}
