package accord.local;

import java.util.Iterator;
import java.util.function.Consumer;
import java.util.stream.Stream;

import accord.txn.Timestamp;
import com.google.common.collect.Iterators;

public abstract class CommandsForKey implements Listener, Iterable<Command>
{
    public interface CommandTimeseries
    {
        Command get(Timestamp timestamp);
        void add(Timestamp timestamp, Command command);
        void remove(Timestamp timestamp);

        /**
         * All commands before (exclusive of) the given timestamp
         */
        Stream<Command> before(Timestamp timestamp);

        /**
         * All commands after (exclusive of) the given timestamp
         */
        Stream<Command> after(Timestamp timestamp);

        /**
         * All commands between (inclusive of) the given timestamps
         */
        Stream<Command> between(Timestamp min, Timestamp max);

        Stream<Command> all();
    }

    public abstract CommandTimeseries uncommitted();
    public abstract CommandTimeseries committedById();
    public abstract CommandTimeseries committedByExecuteAt();

    public abstract Timestamp max();
    public abstract void updateMax(Timestamp timestamp);

    @Override
    public void onChange(Command command)
    {
        updateMax(command.executeAt());
        switch (command.status())
        {
            case Applied:
            case Executed:
            case Committed:
                uncommitted().remove(command.txnId());
                committedById().add(command.txnId(), command);
                committedByExecuteAt().add(command.executeAt(), command);
                command.removeListener(this);
                break;
        }
    }

    public void register(Command command)
    {
        updateMax(command.executeAt());
        uncommitted().add(command.txnId(), command);
        command.addListener(this);
    }

    public void forWitnessed(Timestamp minTs, Timestamp maxTs, Consumer<Command> consumer)
    {
        uncommitted().between(minTs, maxTs)
                .filter(cmd -> cmd.hasBeen(Status.PreAccepted)).forEach(consumer);
        committedById().between(minTs, maxTs).forEach(consumer);
        committedByExecuteAt().between(minTs, maxTs)
                .filter(cmd -> cmd.txnId().compareTo(minTs) < 0 || cmd.txnId().compareTo(maxTs) > 0).forEach(consumer);
    }

    @Override
    public Iterator<Command> iterator()
    {
        return Iterators.concat(uncommitted().all().iterator(), committedByExecuteAt().all().iterator());
    }

    public boolean isEmpty()
    {
        return uncommitted.isEmpty() && committedById.isEmpty();
    }
}
