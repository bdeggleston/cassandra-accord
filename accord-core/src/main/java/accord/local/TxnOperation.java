package accord.local;

import accord.api.Key;
import accord.txn.TxnId;

/**
 * An operation that is executed in the context of a command store
 */
public interface TxnOperation
{
    Iterable<TxnId> expectedTxnIds();
    Iterable<Key> expectedKeys();
}
