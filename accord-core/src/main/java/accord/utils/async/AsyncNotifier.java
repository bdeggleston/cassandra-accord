package accord.utils.async;

import java.util.concurrent.Executor;
import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * Handle for async computations that supports multiple listeners and registering
 * listeners after the computation has started
 */
public interface AsyncNotifier<V>
{
    <T> AsyncNotifier<T> map(Function<V, T> map);
    <T> AsyncNotifier<T> flatMap(Function<? super V, ? extends AsyncNotifier<T>> mapper);

    void listen(BiConsumer<? super V, Throwable> callback);

    default void listen(BiConsumer<? super V, Throwable> callback, Executor executor)
    {
        listen(AsyncCallbacks.inExecutor(callback, executor));
    }

    default void listen(Runnable runnable)
    {
        listen((unused, failure) -> {
            if (failure == null) runnable.run();
            else throw new RuntimeException(failure);
        });
    }

    default void listen(Runnable runnable, Executor executor)
    {
        listen(AsyncCallbacks.inExecutor(runnable, executor));
    }

    default AsyncChain<V> asChain()
    {
        return new AsyncChains.Head<V>()
        {
            @Override
            public void begin(BiConsumer<? super V, Throwable> callback)
            {
                listen(callback);
            }
        };
    }

    boolean isDone();
    boolean isSuccess();

    default void addCallback(BiConsumer<? super V, Throwable> callback)
    {
        listen(callback);
    }

    default void addCallback(BiConsumer<? super V, Throwable> callback, Executor executor)
    {
        listen(callback, executor);
    }

    default void addListener(Runnable runnable)
    {
        listen(runnable);
    }

    default void addListener(Runnable runnable, Executor executor)
    {
        listen(runnable, executor);
    }

    interface Settable<V> extends AsyncNotifier<V>
    {
        boolean trySuccess(V value);

        default void setSuccess(V value)
        {
            if (!trySuccess(value))
                throw new IllegalStateException("Result has already been set on " + this);
        }

        boolean tryFailure(Throwable throwable);

        default void setFailure(Throwable throwable)
        {
            if (!tryFailure(throwable))
                throw new IllegalStateException("Result has already been set on " + this);
        }

        default BiConsumer<V, Throwable> settingCallback()
        {
            return (result, throwable) -> {

                if (throwable == null)
                    trySuccess(result);
                else
                    tryFailure(throwable);
            };
        }
    }
}
