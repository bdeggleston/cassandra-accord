package accord.local;

import java.util.Collections;
import java.util.function.Consumer;

import accord.api.*;
import accord.txn.*;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import org.apache.cassandra.utils.concurrent.Future;

import static accord.local.Status.Accepted;
import static accord.local.Status.Applied;
import static accord.local.Status.Committed;
import static accord.local.Status.Executed;
import static accord.local.Status.PreAccepted;
import static accord.local.Status.ReadyToExecute;

public abstract class Command implements Listener, Consumer<Listener>, TxnOperation
{
    public abstract TxnId txnId();
    public abstract CommandStore commandStore();

    public abstract Txn txn();
    public abstract void txn(Txn txn);

    public abstract Ballot promised();
    public abstract void promised(Ballot ballot);

    public abstract Ballot accepted();
    public abstract void accepted(Ballot ballot);

    public abstract Timestamp executeAt();
    public abstract void executeAt(Timestamp timestamp);

    public abstract Dependencies savedDeps();
    public abstract void savedDeps(Dependencies deps);

    public abstract Writes writes();
    public abstract void writes(Writes writes);

    public abstract Result result();
    public abstract void result(Result result);

    public abstract Status status();
    public abstract void status(Status status);

    public abstract Command addListener(Listener listener);
    public abstract void removeListener(Listener listener);
    public abstract void notifyListeners();

    public abstract void addWaitingOnCommit(Command command);
    public abstract boolean isWaitingOnCommit();
    public abstract void removeWaitingOnCommit(Command command);
    public abstract Command firstWaitingOnCommit();

    public abstract void addWaitingOnApplyIfAbsent(Command command);
    public abstract boolean isWaitingOnApply();
    public abstract void removeWaitingOnApply(Command command);
    public abstract Command firstWaitingOnApply();

    public boolean isUnableToApply()
    {
        return isWaitingOnCommit() || isWaitingOnApply();
    }

    public boolean hasBeen(Status status)
    {
        return status().hasBeen(status);
    }

    public boolean is(Status status)
    {
        return status() == status;
    }

    @Override
    public Iterable<TxnId> txnIds()
    {
        return Iterables.concat(Collections.singleton(txnId()), savedDeps().txnIds());
    }

    @Override
    public Iterable<Key> keys()
    {
        return txn().keys();
    }

    // requires that command != null
    // relies on mutual exclusion for each key
    public boolean witness(Txn txn)
    {
        if (promised().compareTo(Ballot.ZERO) > 0)
            return false;

        if (hasBeen(PreAccepted))
            return true;

        Timestamp max = txn.maxConflict(commandStore());
        // unlike in the Accord paper, we partition shards within a node, so that to ensure a total order we must either:
        //  - use a global logical clock to issue new timestamps; or
        //  - assign each shard _and_ process a unique id, and use both as components of the timestamp
        Timestamp witnessed = txnId().compareTo(max) > 0 && txnId().epoch >= commandStore().epoch() ? txnId() : commandStore().uniqueNow(max);

        txn(txn);
        executeAt(witnessed);
        status(PreAccepted);

        txn.register(commandStore(), this);
        notifyListeners();
        return true;
    }

    public boolean accept(Ballot ballot, Txn txn, Timestamp executeAt, Dependencies deps)
    {
        if (promised().compareTo(ballot) > 0)
            return false;

        if (hasBeen(Committed))
            return false;

        witness(txn);
        savedDeps(deps);
        executeAt(executeAt);
        promised(ballot);
        accepted(ballot);
        status(Accepted);
        notifyListeners();
        return true;
    }

    // relies on mutual exclusion for each key
    public Future<?> commit(Txn txn, Dependencies deps, Timestamp executeAt)
    {
        if (hasBeen(Committed))
        {
            if (executeAt.equals(executeAt()))
                return Write.SUCCESS;

            commandStore().agent().onInconsistentTimestamp(this, executeAt(), executeAt);
        }

        witness(txn);
        savedDeps(deps);
        executeAt(executeAt);
        status(Committed);
        Preconditions.checkState(!isWaitingOnCommit());
        Preconditions.checkState(!isWaitingOnApply());

        for (TxnId id : savedDeps().on(commandStore()))
        {
            Command command = commandStore().command(id);
            switch (command.status())
            {
                default:
                    throw new IllegalStateException();
                case NotWitnessed:
                    command.witness(deps.get(command.txnId()));
                case PreAccepted:
                case Accepted:
                    // we don't know when these dependencies will execute, and cannot execute until we do
                    addWaitingOnCommit(command);
                    command.addListener(this);
                    break;
                case Committed:
                    // TODO: split into ReadyToRead and ReadyToWrite;
                    //       the distributed read can be performed as soon as those keys are ready, and in parallel with any other reads
                    //       the client can even ACK immediately after; only the write needs to be postponed until other in-progress reads complete
                case ReadyToExecute:
                case Executed:
                case Applied:
                    command.addListener(this);
                    updatePredecessor(command);
                    break;
            }
        }
        notifyListeners();
        return maybeExecute();
    }

    public Future<?> apply(Txn txn, Dependencies deps, Timestamp executeAt, Writes writes, Result result)
    {
        if (hasBeen(Executed) && executeAt.equals(executeAt()))
            return Write.SUCCESS;
        else if (!hasBeen(Committed))
            commit(txn, deps, executeAt);
        else if (!executeAt.equals(executeAt()))
            commandStore().agent().onInconsistentTimestamp(this, executeAt(), executeAt);

        executeAt(executeAt);
        writes(writes);
        result(result);
        status(Executed);
        notifyListeners();
        return maybeExecute();
    }

    public boolean recover(Txn txn, Ballot ballot)
    {
        if (promised().compareTo(ballot) > 0)
            return false;

        witness(txn);
        promised(ballot);
        return true;
    }

    @Override
    public void onChange(Command command)
    {
        switch (command.status())
        {
            case Committed:
            case ReadyToExecute:
            case Executed:
            case Applied:
                if (isUnableToApply())
                {
                    updatePredecessor(command);
                    if (isWaitingOnCommit())
                    {
                        removeWaitingOnCommit(command);
                    }
                }
                else
                {
                    command.removeListener(this);
                }
                maybeExecute();
                break;
        }
    }

    protected void postApply()
    {
        status(Applied);
        notifyListeners();
    }

    protected Future<?> apply()
    {
        return writes().apply(commandStore()).flatMap(unused ->
            commandStore().process(this, commandStore -> {
                postApply();
            })
        );
    }

    public Read.ReadFuture read(Keys keyscope)
    {
        return txn().read(this, keyscope);
    }

    private Future<?> maybeExecute()
    {
        if (status() != Committed && status() != Executed)
            return Write.SUCCESS;

        if (isUnableToApply())
            return Write.SUCCESS;

        switch (status())
        {
            case Committed:
                // TODO: maintain distinct ReadyToRead and ReadyToWrite states
                status(ReadyToExecute);
                notifyListeners();
                break;
            case Executed:
                return apply();
        }
        return Write.SUCCESS;
    }

    private void updatePredecessor(Command committed)
    {
        if (committed.executeAt().compareTo(executeAt()) > 0)
        {
            // cannot be a predecessor if we execute later
            committed.removeListener(this);
        }
        else if (committed.hasBeen(Applied))
        {
            removeWaitingOnApply(committed);
            committed.removeListener(this);
        }
        else
        {
            addWaitingOnApplyIfAbsent(committed);
        }
    }

    public Command blockedBy()
    {
        Command cur = directlyBlockedBy();
        if (cur == null)
            return null;

        Command next;
        while (null != (next = cur.directlyBlockedBy()))
            cur = next;
        return cur;
    }

    private Command directlyBlockedBy()
    {
        // firstly we're waiting on every dep to commit
        while (isWaitingOnCommit())
        {
            // TODO: when we change our liveness mechanism this may not be a problem
            // cannot guarantee that listener updating this set is invoked before this method by another listener
            // so we must check the entry is still valid, and potentially remove it if not
            Command waitingOn = firstWaitingOnCommit();
            if (!waitingOn.hasBeen(Committed)) return waitingOn;
            onChange(waitingOn);
        }

        while (isWaitingOnApply())
        {
            // TODO: when we change our liveness mechanism this may not be a problem
            // cannot guarantee that listener updating this set is invoked before this method by another listener
            // so we must check the entry is still valid, and potentially remove it if not
            Command waitingOn = firstWaitingOnApply();
            if (!waitingOn.hasBeen(Applied)) return waitingOn;
            onChange(waitingOn);
        }

        return null;
    }

    @Override
    public void accept(Listener listener)
    {
        listener.onChange(this);
    }

    @Override
    public String toString()
    {
        return "Command{" +
               "txnId=" + txnId() +
               ", status=" + status() +
               ", txn=" + txn() +
               ", executeAt=" + executeAt() +
               ", deps=" + savedDeps() +
               '}';
    }
}
