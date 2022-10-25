package accord.utils.async;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.IntFunction;

public abstract class AsyncNotifierCombiner<I, O> extends AsyncNotifiers.Settable<O>
{
    private static final AtomicIntegerFieldUpdater<AsyncNotifierCombiner> REMAINING = AtomicIntegerFieldUpdater.newUpdater(AsyncNotifierCombiner.class, "remaining");
    private final I[] results;
    private volatile int remaining;

    protected AsyncNotifierCombiner(List<? extends AsyncNotifier<? extends I>> inputs)
    {
        Preconditions.checkArgument(!inputs.isEmpty());
        this.results = (I[]) new Object[inputs.size()];
        this.remaining = inputs.size();
        for (int i=0, mi=inputs.size(); i<mi; i++)
            inputs.get(i).addCallback(callbackFor(i));
    }

    abstract void complete(I[] results);

    private void callback(int idx, I result, Throwable throwable)
    {
        int current = remaining;
        if (current == 0)
            return;

        if (throwable != null && REMAINING.compareAndSet(this, current, 0))
        {
            tryFailure(throwable);
            return;
        }

        results[idx] = result;
        if (REMAINING.decrementAndGet(this) == 0)
        {
            try
            {
                complete(results);
            }
            catch (Throwable t)
            {
                tryFailure(t);
            }
        }
    }

    private BiConsumer<I, Throwable> callbackFor(int idx)
    {
        return (result, failure) -> callback(idx, result, failure);
    }

    static class All<V> extends AsyncNotifierCombiner<V, List<V>>
    {
        public All(List<? extends AsyncNotifier<? extends V>> inputs)
        {
            super(inputs);
        }

        @Override
        void complete(V[] results)
        {
            List<V> result = Lists.newArrayList(results);
            trySuccess(result);
        }
    }

    static class Reduce<V> extends AsyncNotifierCombiner<V, V>
    {
        private final BiFunction<V, V, V> reducer;
        Reduce(List<? extends AsyncNotifier<? extends V>> asyncChains, BiFunction<V, V, V> reducer)
        {
            super(asyncChains);
            this.reducer = reducer;
        }

        @Override
        void complete(V[] results)
        {
            V result = results[0];
            for (int i=1; i< results.length; i++)
                result = reducer.apply(result, results[i]);
            trySuccess(result);
        }
    }
}
