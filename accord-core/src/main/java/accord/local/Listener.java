package accord.local;

public interface Listener extends TxnOperation
{
    void onChange(Command command);

    /**
     * Indicates the command to listener relationship doesn't need to persist across restarts
     */
    default boolean isTransient()
    {
        return false;
    }
}
