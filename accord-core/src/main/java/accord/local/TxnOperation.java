package accord.local;

import accord.api.Key;
import accord.txn.Txn;
import accord.txn.TxnId;

import java.util.Collections;

/**
 * An operation that is executed in the context of a command store. The methods communicate to the implementation which
 * commands and commandsPerKey items will be needed to run the operation
 */
public interface TxnOperation
{
    Iterable<TxnId> txnIds();
    Iterable<Key> keys();
}
