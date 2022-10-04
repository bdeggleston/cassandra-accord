package accord.utils;

import com.google.common.base.Preconditions;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.function.BiConsumer;

import static java.util.concurrent.atomic.AtomicLongFieldUpdater.newUpdater;

public interface MultiCallback<T>
{
    void onComplete(T result, Throwable failure);

    BiConsumer<T, Throwable> registerAndGet();
    void listen();

    int registered();

    abstract class LockFree implements MultiCallback<Void>
    {
        private static final long COMPLETED_FLAG = 1L<<63;
        private static final long LISTENING_FLAG = 1L<<62;
        private static final int COUNT_WIDTH = 30;
        private static final int REGISTERED_OFFSET = 0;
        private static final int RESPONSES_OFFSET = COUNT_WIDTH;
        private static final long COUNT_MASK;
        static
        {
            long maxVal = 0;
            for (int i=0; i<COUNT_WIDTH; i++)
                maxVal |= (1L<<i);
            COUNT_MASK = maxVal;
        }

        private static final AtomicLongFieldUpdater<LockFree> STATUS_UPDATER = newUpdater(LockFree.class, "status");
        private volatile long status = 0;

        private static boolean flagIsSet(long value, long flag)
        {
            return (value & flag) != 0;
        }

        private static long setFlag(long current, long flag)
        {
            return current | flag;
        }

        private static long valueAtOffset(long value, int offset)
        {
            return (value >>> offset) & COUNT_MASK;
        }

        private static long setValueAtOffset(long current, long value, int offset)
        {
            Preconditions.checkArgument(value <= COUNT_MASK);
            Preconditions.checkArgument(value >= 0);
            current ^= (COUNT_MASK) << offset;
            current |= value << offset;
            return current;
        }

        private static boolean shouldComplete(long registered, long responded, boolean isListening)
        {
            Preconditions.checkArgument(responded >= registered);
            return (responded == registered) && isListening;
        }

        private void callback(Void result, Throwable throwable)
        {
            while (true)
            {
                long current = status;
                if (flagIsSet(current, COMPLETED_FLAG))
                    return;
                boolean listening = flagIsSet(current, LISTENING_FLAG);
                long registered = valueAtOffset(current, REGISTERED_OFFSET);
                long responded = valueAtOffset(current, RESPONSES_OFFSET) + 1;
                boolean complete = throwable != null || shouldComplete(registered, responded, listening);

                long update = setValueAtOffset(current, responded, RESPONSES_OFFSET);
                if (complete)
                    update = setFlag(update, COMPLETED_FLAG);

                if (STATUS_UPDATER.compareAndSet(this, current, update))
                {
                    if (complete)
                        onComplete(null, throwable);
                    return;
                }
            }
        }

        @Override
        public BiConsumer<Void, Throwable> registerAndGet()
        {
            while (true)
            {
                long current = status;
                Preconditions.checkState(!flagIsSet(current, LISTENING_FLAG));
                long registered = valueAtOffset(current, REGISTERED_OFFSET) + 1;
                long update = setValueAtOffset(current, registered, REGISTERED_OFFSET);

                if (STATUS_UPDATER.compareAndSet(this, current, update))
                    break;
            }
            return this::callback;
        }

        @Override
        public void listen()
        {
            while (true)
            {
                long current = status;
                if (flagIsSet(current, COMPLETED_FLAG))
                    return;

                long registered = valueAtOffset(current, REGISTERED_OFFSET);
                long responded = valueAtOffset(current, RESPONSES_OFFSET);
                boolean complete = shouldComplete(registered, responded, true);
                long update = setFlag(current, LISTENING_FLAG);
                if (complete)
                    update = setFlag(current, COMPLETED_FLAG);

                if (STATUS_UPDATER.compareAndSet(this, current, update))
                {
                    if (complete)
                        onComplete(null, null);
                    return;
                }
            }
        }

        @Override
        public int registered()
        {
            return (int) valueAtOffset(status, REGISTERED_OFFSET);
        }
    }

    abstract class ValueMerging<T> implements MultiCallback<T>
    {
        private int registered = 0;
        private int responses = 0;
        private boolean completed = false;
        private boolean listening = false;

        protected abstract void accumulateResult(T result);
        protected abstract T result();

        private void complete(T result, Throwable throwable)
        {
            completed = true;
            onComplete(result, throwable);
        }

        private void maybeComplete()
        {
            if (responses == registered && listening)
                complete(result(), null);
        }

        private synchronized void callback(T result, Throwable failure)
        {
            if (completed)
                return;

            Preconditions.checkState(responses < registered);
            responses++;

            if (failure != null)
            {
                complete(null, failure);
                return;
            }
            accumulateResult(result);
            maybeComplete();
        }

        @Override
        public synchronized BiConsumer<T, Throwable> registerAndGet()
        {
            Preconditions.checkState(!listening);
            registered++;
            return this::callback;
        }

        /**
         * Call after all calls to registerAndGet have completed
         */
        @Override
        public synchronized void listen()
        {
            // exceptions that already arrived could have set isDone
            if (completed)
                return;

            listening = true;

            // all instances would have called `get` by now, if they all completed, complete the callback
            maybeComplete();
        }

        @Override
        public synchronized int registered()
        {
            return registered;
        }

    }
}
