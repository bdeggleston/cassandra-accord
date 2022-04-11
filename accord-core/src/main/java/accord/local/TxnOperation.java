package accord.local;

import accord.api.Key;
import accord.txn.TxnId;

import java.util.Collections;

/**
 * An operation that is executed in the context of a command store. The methods are intended to
 * communicate to the implementation which commands and commandsPerKey items are expected to be
 * needed to run the operation
 */
public interface TxnOperation
{
    TxnId txnId();
    Iterable<Key> keys();
    default Iterable<TxnId> depsIds()
    {
        return Collections.emptyList();
    }
}
