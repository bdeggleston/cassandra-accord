/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package accord.impl.mock;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import accord.api.Data;
import accord.api.DataStore;
import accord.api.Key;
import accord.api.Query;
import accord.api.Read;
import accord.api.Result;
import accord.api.Update;
import accord.api.Write;
import accord.local.Node;
import accord.local.Node.Id;
import accord.local.SafeCommandStore;
import accord.primitives.Keys;
import accord.primitives.Ranges;
import accord.primitives.Seekable;
import accord.primitives.Seekables;
import accord.primitives.SyncPoint;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.Writes;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;
import accord.utils.async.AsyncResults;

import static java.util.Collections.synchronizedList;
import static org.junit.jupiter.api.Assertions.assertNull;

public class MockStore implements DataStore
{
    private final Id nodeId;

    public MockStore(Id nodeId)
    {
        this.nodeId = nodeId;
    }

    public static class MockData extends ArrayList<Id> implements Data, Result
    {
        private Seekables keys;

        public MockData()
        {
            this(null, null);
        }

        public MockData(Id id, Seekables keys)
        {
            if (id != null)
                add(id);
            if (keys == null)
                this.keys = Keys.EMPTY;
            else
                this.keys = keys;
        }

        @Override
        public Data merge(Data data)
        {
            MockData mockData = (MockData)data;
            addAll(mockData);
            this.keys = keys.with(mockData.keys);
            return this;
        }
    };

    public static final MockData EMPTY_DATA = new MockData();
    public static final Result RESULT = EMPTY_DATA;
    public static final Query QUERY = (txnId, executeAt, keys, data, read, update) -> RESULT;
    public static final Query QUERY_RETURNING_INPUT = (txnId, executeAt, keys, data, read, update) -> (MockData)data;

    public static Read read(Seekables<?, ?> keys)
    {
        return read(keys, null);
    }

    public static Read read(Seekables<?, ?> keys, Consumer<Boolean> digestReadListener)
    {
        return new MockRead(keys, digestReadListener);
    }

    public static class MockRead implements Read
    {
        public final Seekables keys;
        public final Consumer<Boolean> digestReadListener;

        public MockRead(Seekables keys, Consumer<Boolean> digestReadListener)
        {
            this.keys = keys;
            this.digestReadListener = digestReadListener;
        }

        @Override
        public Seekables<?, ?> keys()
        {
            return keys;
        }

        @Override
        public AsyncChain<Data> read(Seekable key, Txn.Kind kind, SafeCommandStore commandStore, Timestamp executeAt, DataStore store)
        {
            // FIXME: remove
            if (digestReadListener != null)
                digestReadListener.accept(false);
            return AsyncChains.success(new MockData(((MockStore)store).nodeId, Seekables.of(key)));
        }

        @Override
        public Read slice(Ranges ranges)
        {
            return MockStore.read(keys.slice(ranges), digestReadListener);
        }

        @Override
        public Read merge(Read other)
        {
            return MockStore.read(((Seekables)keys).with(other.keys()), digestReadListener);
        }

        @Override
        public String toString()
        {
            return keys.toString();
        }
    }

    public static class MockWrite implements Write
    {
        public final List<Key> appliedKeys = synchronizedList(new ArrayList<>());
        public final boolean isEmpty;

        public MockWrite(boolean isEmpty)
        {
            this.isEmpty = isEmpty;
        }

        @Override
        public synchronized AsyncChain<Void> apply(Seekable key, SafeCommandStore safeStore, Timestamp executeAt, DataStore store)
        {
            appliedKeys.add((Key)key);
            return AsyncChains.success(null);
        }

        public boolean isEmpty()
        {
            return isEmpty;
        }
    }

    public static class MockUpdate implements Update
    {
        final Write write;
        final Seekables keys;

        Data data;

        public MockUpdate(Seekables keys, Write write)
        {
            this.keys = keys;
            this.write = write;
        }

        @Override
        public Seekables<?, ?> keys()
        {
            return keys;
        }

            @Override
            public Write apply(Timestamp executeAt, Data data)
            {
                assertNull(this.data);
                this.data = data;
                return write;
            }

        @Override
        public Update slice(Ranges ranges)
        {
            return this;
        }

        @Override
        public Update merge(Update other)
        {
            return this;
        }
    }

    public static Update update(Seekables<?, ?> keys)
    {
        Write write = new Write()
        {

            @Override
            public AsyncChain<Void> apply(Seekable key, SafeCommandStore safeStore, Timestamp executeAt, DataStore store)
            {
                return Writes.SUCCESS;
            }

            @Override
            public boolean isEmpty()
            {
                return keys.isEmpty();
            }
        };
        return new MockUpdate(keys, write);
    }

    static class ImmediateFetchFuture extends AsyncResults.SettableResult<Ranges> implements FetchResult
    {
        ImmediateFetchFuture(Ranges ranges) { setSuccess(ranges); }
        @Override public void abort(Ranges abort) { }
    }

    @Override
    public FetchResult fetch(Node node, SafeCommandStore safeStore, Ranges ranges, SyncPoint syncPoint, FetchRanges callback)
    {
        callback.starting(ranges).started(Timestamp.NONE);
        callback.fetched(ranges);
        return new ImmediateFetchFuture(ranges);
    }
}
