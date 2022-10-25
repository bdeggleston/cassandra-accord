package accord.utils.async;

import com.google.common.base.Preconditions;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

public class AsyncNotifiers
{
    private AsyncNotifiers() {}

    private static class Result<V>
    {
        final V value;
        final Throwable failure;

        public Result(V value, Throwable failure)
        {
            this.value = value;
            this.failure = failure;
        }
    }

    static class AbstractNotifier<V> implements AsyncNotifier<V>
    {
        private static final AtomicReferenceFieldUpdater<AbstractNotifier, Object> STATE = AtomicReferenceFieldUpdater.newUpdater(AbstractNotifier.class, Object.class, "state");

        private volatile Object state;

        private static class Listener<V>
        {
            final BiConsumer<? super V, Throwable> callback;
            Listener<V> next;

            public Listener(BiConsumer<? super V, Throwable> callback)
            {
                this.callback = callback;
            }
        }

        private void notify(Listener<V> listener, Result<V> result)
        {
            while (listener != null)
            {
                listener.callback.accept(result.value, result.failure);
                listener = listener.next;
            }
        }

        boolean trySetResult(Result<V> result)
        {
            while (true)
            {
                Object current = state;
                if (current instanceof Result)
                    return false;
                Listener<V> listener = (Listener<V>) current;
                if (STATE.compareAndSet(this, current, result))
                {
                    notify(listener, result);
                    return true;
                }
            }
        }

        boolean trySetResult(V result, Throwable failure)
        {
            return trySetResult(new Result<>(result, failure));
        }

        void setResult(Result<V> result)
        {
            if (!trySetResult(result))
                throw new IllegalStateException("Result has already been set on " + this);
        }

        void setResult(V result, Throwable failure)
        {
            if (!trySetResult(result, failure))
                throw new IllegalStateException("Result has already been set on " + this);
        }

        @Override
        public void listen(BiConsumer<? super V, Throwable> callback)
        {
            Listener<V> listener = null;
            while (true)
            {
                Object current = state;
                if (current instanceof Result)
                {
                    Result<V> result = (Result<V>) current;
                    callback.accept(result.value, result.failure);
                    return;
                }
                if (listener == null)
                    listener = new Listener<>(callback);

                listener.next = (Listener<V>) current;
                if (STATE.compareAndSet(this, current, listener))
                    return;
            }
        }

        @Override
        public boolean isDone()
        {
            return state instanceof Result;
        }

        @Override
        public boolean isSuccess()
        {
            Object current = state;
            return current instanceof Result && ((Result) current).failure == null;
        }

        @Override
        public <T> AsyncNotifier<T> map(Function<V, T> map)
        {
            return new Map<>(this, map);
        }

        @Override
        public <T> AsyncNotifier<T> flatMap(Function<? super V, ? extends AsyncNotifier<T>> mapper)
        {
            return new FlatMap<V, T>(this, mapper);
        }
    }

    static class Map<I, O> extends AbstractNotifier<O>
    {
        Function<I, O> map;

        public Map(AsyncNotifier<I> notifier, Function<I, O> map)
        {
            this.map = map;
            notifier.listen(this::mapResult);
        }

        private void mapResult(I result, Throwable throwable)
        {
            setResult(map.apply(result), throwable);
        }
    }

    static class FlatMap<I, O> extends AbstractNotifier<O>
    {
        Function<? super I, ? extends AsyncNotifier<O>> map;

        public FlatMap(AbstractNotifier<I> notifier, Function<? super I, ? extends AsyncNotifier<O>> map)
        {
            this.map = map;
            notifier.listen(this::mapResult);
        }

        private void mapResult(I result, Throwable failure)
        {
            if (failure != null) setResult(null, failure);
            else map.apply(result).listen(this::setResult);
        }
    }

    static class Chain<V> extends AbstractNotifier<V>
    {
        public Chain(AsyncChain<V> chain)
        {
            chain.begin(this::setResult);
        }
    }

    public static class Settable<V> extends AbstractNotifier<V> implements AsyncNotifier.Settable<V>
    {

        @Override
        public boolean trySuccess(V value)
        {
            return trySetResult(value, null);
        }

        @Override
        public boolean tryFailure(Throwable throwable)
        {
            return trySetResult(null, throwable);
        }
    }

    static class Immediate<V> implements AsyncNotifier<V>
    {
        private final V value;
        private final Throwable failure;

        Immediate(V value)
        {
            this.value = value;
            this.failure = null;
        }

        Immediate(Throwable failure)
        {
            this.value = null;
            this.failure = failure;
        }

        @Override
        public void listen(BiConsumer<? super V, Throwable> callback)
        {
            callback.accept(value, failure);
        }

        @Override
        public <T> AsyncNotifier<T> map(Function<V, T> map)
        {
            return new Map<>(this, map);
        }

        @Override
        public <T> AsyncNotifier<T> flatMap(Function<? super V, ? extends AsyncNotifier<T>> mapper)
        {
            if (failure != null)
                return (AsyncNotifier<T>) this;
            return mapper.apply(value);
        }

        @Override
        public boolean isDone()
        {
            return true;
        }

        @Override
        public boolean isSuccess()
        {
            return failure == null;
        }
    }

    /**
     * Creates a notifier for the given chain. This calls begin on the supplied chain
     */
    public static <V> AsyncNotifier<V> forChain(AsyncChain<V> chain)
    {
        return new Chain<>(chain);
    }

    public static <V> AsyncNotifier<V> success(V value)
    {
        return new Immediate<>(value);
    }

    public static <V> AsyncNotifier<V> failure(Throwable failure)
    {
        return new Immediate<>(failure);
    }

    public static <V> AsyncNotifier<V> ofCallable(Executor executor, Callable<V> callable)
    {
        Settable<V> notifier = new Settable<V>();
        executor.execute(() -> {
            try
            {
                notifier.trySuccess(callable.call());
            }
            catch (Exception e)
            {
                notifier.tryFailure(e);
            }
        });
        return notifier;
    }

    public static AsyncNotifier<Void> ofRunnable(Executor executor, Runnable runnable)
    {
        Settable<Void> notifier = new Settable<Void>();
        executor.execute(() -> {
            try
            {
                runnable.run();
                notifier.trySuccess(null);
            }
            catch (Exception e)
            {
                notifier.tryFailure(e);
            }
        });
        return notifier;
    }

    public static <V> AsyncNotifier.Settable<V> settable()
    {
        return new Settable<>();
    }

    public static <V> AsyncNotifier<List<V>> all(List<? extends AsyncNotifier<? extends V>> notifiers)
    {
        Preconditions.checkArgument(!notifiers.isEmpty());
        return new AsyncNotifierCombiner.All<>(notifiers);
    }

    public static <V> AsyncNotifier<? extends V> reduce(List<? extends AsyncNotifier<? extends V>> notifiers, BiFunction<V, V, V> reducer)
    {
        Preconditions.checkArgument(!notifiers.isEmpty());
        if (notifiers.size() == 1)
            return notifiers.get(0);
        return new AsyncNotifierCombiner.Reduce<>(notifiers, reducer);
    }

    public static <V> V getBlocking(AsyncNotifier<V> notifier) throws InterruptedException
    {
        AtomicReference<Result<V>> callbackResult = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        notifier.addCallback((result, failure) -> {
            callbackResult.set(new Result(result, failure));
            latch.countDown();
        });

        latch.await();
        Result<V> result = callbackResult.get();
        if (result.failure == null) return result.value;
        else throw new RuntimeException(result.failure);
    }

    public static <V> V getUninterruptibly(AsyncNotifier<V> notifier)
    {
        try
        {
            return getBlocking(notifier);
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
    }

    public static <V> void awaitUninterruptibly(AsyncNotifier<V> notifier)
    {
        getUninterruptibly(notifier);
    }
}
