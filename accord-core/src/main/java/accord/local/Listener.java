package accord.local;

import accord.primitives.TxnId;

public interface Listener
{
    void onChange(Command command);

    /**
     * Scope needed to run onChange
     */
    TxnOperation listenerScope(TxnId caller);

    /**
     * Indicates the command to listener relationship doesn't need to persist across restarts
     */
    default boolean isTransient()
    {
        return false;
    }
}
