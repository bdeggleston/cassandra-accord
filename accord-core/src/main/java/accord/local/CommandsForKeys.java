package accord.local;

import accord.api.Key;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommandsForKeys
{
    private static final Logger logger = LoggerFactory.getLogger(CommandsForKey.class);

    private CommandsForKeys() {}

    static boolean register(SafeCommandStore safeStore, CommandsForKey cfk, Command command)
    {
        boolean listenToCommand = false;
        CommandsForKey.Update update = safeStore.beginUpdate(cfk);
        update.updateMax(command.executeAt());

        if (command.hasBeen(Status.Committed))
        {
            update.addCommitted(command);
        }
        else if (command.status() != Status.Invalidated)
        {
            update.addUncommitted(command);
            listenToCommand = true;
        }
        update.complete();
        return listenToCommand;
    }

    static void register(SafeCommandStore safeStore, Key key, Command command)
    {
        register(safeStore, safeStore.commandsForKey(key), command);
    }

    public static void listenerUpdate(SafeCommandStore safeStore, CommandsForKey listener, Command command)
    {
        if (logger.isTraceEnabled())
            logger.trace("[{}]: updating as listener in response to change on {} with status {} ({})",
                         listener.key(), command.txnId(), command.status(), command);
        CommandsForKey.Update update = safeStore.beginUpdate(listener);
        update.updateMax(command.executeAt());
        switch (command.status())
        {
            case Applied:
            case PreApplied:
            case Committed:
                update.addCommitted(command);
            case Invalidated:
                update.removeUncommitted(command);

                Command.removeListener(safeStore, command, CommandsForKey.listener(listener.key()));
                break;
        }
        update.complete();
    }
}
