package accord.local;

import java.util.function.Consumer;

import accord.api.Result;
import accord.txn.Ballot;
import accord.txn.Dependencies;
import accord.txn.Timestamp;
import accord.txn.Txn;
import accord.txn.TxnId;
import accord.txn.Writes;

import static accord.local.Status.Accepted;
import static accord.local.Status.Applied;
import static accord.local.Status.Committed;
import static accord.local.Status.Executed;
import static accord.local.Status.PreAccepted;
import static accord.local.Status.ReadyToExecute;

public abstract class Command implements Listener, Consumer<Listener>
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

    public abstract void clearWaitingOnCommit();
    public abstract void addWaitingOnCommit(TxnId txnId, Command command);
    public abstract boolean isWaitingOnCommit();
    public abstract boolean removeWaitingOnCommit(TxnId txnId);
    public abstract Command firstWaitingOnCommit();

    public abstract void clearWaitingOnApply();
    public abstract void addWaitingOnApplyIfAbsent(Timestamp txnId, Command command);
    public abstract boolean isWaitingOnApply();
    public abstract boolean removeWaitingOnApply(Timestamp txnId);
    public abstract Command firstWaitingOnApply();

    public boolean hasBeen(Status status)
    {
        return status().compareTo(status) >= 0;
    }

    public boolean is(Status status)
    {
        return status() == status;
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
    public boolean commit(Txn txn, Dependencies deps, Timestamp executeAt)
    {
        if (hasBeen(Committed))
        {
            if (executeAt.equals(executeAt()))
                return false;

            commandStore().agent().onInconsistentTimestamp(this, executeAt(), executeAt);
        }

        witness(txn);
        status(Committed);
        savedDeps(deps);
        executeAt(executeAt);
        clearWaitingOnCommit();
        clearWaitingOnApply();

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
                    addWaitingOnCommit(id, command);
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
        // TODO: except for above, explicit waiting on clearing can probably be removed
        //   was probably to prevent having to write != null && !isEmpty everywhere
        if (!isWaitingOnCommit())
        {
            clearWaitingOnCommit();
            if (!isWaitingOnApply())
                clearWaitingOnApply();
        }
        notifyListeners();
        maybeExecute();
        return true;
    }

    public boolean apply(Txn txn, Dependencies deps, Timestamp executeAt, Writes writes, Result result)
    {
        if (hasBeen(Executed) && executeAt.equals(executeAt()))
            return false;
        else if (!hasBeen(Committed))
            commit(txn, deps, executeAt);
        else if (!executeAt.equals(executeAt()))
            commandStore().agent().onInconsistentTimestamp(this, executeAt(), executeAt);

        executeAt(executeAt);
        writes(writes);
        result(result);
        status(Executed);
        notifyListeners();
        maybeExecute();
        return true;
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
                if (isWaitingOnApply())
                {
                    updatePredecessor(command);
                    if (isWaitingOnCommit())
                    {
                        if (removeWaitingOnCommit(command.txnId()) && !isWaitingOnCommit())
                            clearWaitingOnCommit();
                    }
                    if (!isWaitingOnCommit() && !isWaitingOnApply())
                        clearWaitingOnApply();
                }
                else
                {
                    command.removeListener(this);
                }
                maybeExecute();
                break;
        }
    }

    private void maybeExecute()
    {
        if (status() != Committed && status() != Executed)
            return;

        if (isWaitingOnApply())
            return;

        switch (status())
        {
            case Committed:
                // TODO: maintain distinct ReadyToRead and ReadyToWrite states
                status(ReadyToExecute);
                notifyListeners();
                break;
            case Executed:
                writes().apply(commandStore());
                status(Applied);
                notifyListeners();
        }
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
            removeWaitingOnApply(committed.executeAt());
            committed.removeListener(this);
        }
        else
        {
            addWaitingOnApplyIfAbsent(committed.executeAt(), committed);
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
