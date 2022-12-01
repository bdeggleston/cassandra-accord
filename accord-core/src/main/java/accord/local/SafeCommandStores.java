package accord.local;

import com.google.common.collect.ImmutableMap;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public class SafeCommandStores
{
    private SafeCommandStores() {}

    public static class ContextState<Key, Value, Update>
    {
        private final ImmutableMap<Key, Value> originals;
        private final Map<Key, Value> current;
        private final Map<Key, Update> pendingUpdates = new HashMap<>();
        private boolean completed = false;

        private void checkState()
        {
            if (completed)
                throw new IllegalStateException("Context has been closed");
        }

        public ContextState(Iterable<? extends Key> keys, Function<? super Key, Value> globalState, Function<? super Key, Value> initFunction)
        {
            ImmutableMap.Builder<Key, Value> originals = ImmutableMap.builder();
            HashMap<Key, Value> current = new HashMap<>();
            for (Key key : keys)
            {
                Value value = globalState.apply(key);
                if (value == null)
                    value = initFunction.apply(key);
                else
                    originals.put(key, value);
                current.put(key, value);
            }

            this.originals = originals.build();
            this.current = current;
        }

        public Value get(Key key)
        {
            checkState();
            return current.get(key);
        }

        public Update beginUpdate(Key key, Value item, Function<Value, Update> factory)
        {
            checkState();
            if (current.get(key) != item)
                throw new IllegalStateException(String.format("Attempting to update invalid state for %s", key));

            if (pendingUpdates.containsKey(key))
                throw new IllegalStateException(String.format("Update already in progress for %s", key));

            Update update = factory.apply(item);
            pendingUpdates.put(key, update);
            return update;
        }

        public void completeUpdate(Key key, Update update, Value prev, Value next)
        {
            checkState();
            Update pending = pendingUpdates.remove(key);
            if (pending == null)
                throw new IllegalStateException(String.format("Update doesn't exist for %s", key));
            if (pending != update)
                throw new IllegalStateException(String.format("Update was already completed for %s", key));
            if (current.get(key) != prev)
                throw new IllegalStateException(String.format("Current value for %s differs from expected value (%s != %s)", key, current.get(key), prev));
            current.put(key, next);
        }

        public void completeAndUpdateGlobalState(Map<Key, Value> globalState)
        {
            checkState();
            if (!pendingUpdates.isEmpty())
                throw new IllegalStateException("Pending updates left uncompleted: " + pendingUpdates);
            for (Map.Entry<Key, Value> entry : originals.entrySet())
            {
                Value global = globalState.get(entry.getKey());
                Value original = entry.getValue();
                if (global != original)
                    throw new IllegalStateException("A conflicting operation has concurrently updated " + entry.getKey());
            }
            globalState.putAll(current);
            completed = true;
        }
    }
}
