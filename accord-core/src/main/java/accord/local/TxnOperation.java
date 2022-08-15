package accord.local;

import accord.api.Key;
import accord.txn.TxnId;

import java.util.Collections;

/**
 * An operation that is executed in the context of a command store
 */
public interface TxnOperation
{
    Iterable<TxnId> expectedTxnIds();
    Iterable<Key> expectedKeys();

    static TxnOperation scopeFor(Iterable<TxnId> txnIds, Iterable<Key> keys)
    {
        return new TxnOperation()
        {
            @Override
            public Iterable<TxnId> expectedTxnIds() { return txnIds; }

            @Override
            public Iterable<Key> expectedKeys() { return keys; }
        };
    }

    static TxnOperation scopeFor(TxnId txnId, Iterable<Key> keys)
    {
        return scopeFor(Collections.singleton(txnId), keys);
    }

    static TxnOperation scopeFor(TxnId txnId)
    {
        return scopeFor(Collections.singleton(txnId), Collections.emptyList());
    }
}
