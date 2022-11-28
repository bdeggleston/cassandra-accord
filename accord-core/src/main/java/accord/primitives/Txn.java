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

package accord.primitives;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import accord.local.Command;

import accord.api.*;
import accord.local.SafeCommandStore;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public interface Txn
{
    enum Kind
    {
        READ, WRITE;

        public boolean isWrite()
        {
            return this == WRITE;
        }
    }

    class InMemory implements Txn
    {
        private final Kind kind;
        private final Keys keys;
        private final Read read;
        private final Query query;
        private final Update update;

        public InMemory(@Nonnull Keys keys, @Nonnull Read read, @Nonnull Query query)
        {
            this.kind = Kind.READ;
            this.keys = keys;
            this.read = read;
            this.query = query;
            this.update = null;
        }

        public InMemory(@Nonnull Keys keys, @Nonnull Read read, @Nonnull Query query, @Nullable Update update)
        {
            this.kind = Kind.WRITE;
            this.keys = keys;
            this.read = read;
            this.update = update;
            this.query = query;
        }

        protected InMemory(@Nonnull Kind kind, @Nonnull Keys keys, @Nonnull Read read, @Nullable Query query, @Nullable Update update)
        {
            this.kind = kind;
            this.keys = keys;
            this.read = read;
            this.update = update;
            this.query = query;
        }

        @Override
        public PartialTxn slice(KeyRanges ranges, boolean includeQuery)
        {
            return new PartialTxn.InMemory(
                    ranges, kind(), keys().slice(ranges),
                    read().slice(ranges), includeQuery ? query() : null,
                    update() == null ? null : update().slice(ranges)
            );
        }

        @Override
        public Kind kind()
        {
            return kind;
        }

        @Override
        public Keys keys()
        {
            return keys;
        }

        @Override
        public Read read()
        {
            return read;
        }

        @Override
        public Query query()
        {
            return query;
        }

        @Override
        public Update update()
        {
            return update;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Txn txn = (Txn) o;
            return kind() == txn.kind()
                    && keys().equals(txn.keys())
                    && read().equals(txn.read())
                    && Objects.equals(query(), txn.query())
                    && Objects.equals(update(), txn.update());
        }

        @Override
        public int hashCode()
        {
            throw new UnsupportedOperationException();
        }

        public String toString()
        {
            return "{read:" + read() + (update() != null ? ", update:" + update() : "") + '}';
        }
    }

    @Nonnull Kind kind();
    @Nonnull Keys keys();
    @Nonnull Read read();
    @Nullable Query query(); // may be null only in PartialTxn
    @Nullable Update update();

    @Nonnull PartialTxn slice(KeyRanges ranges, boolean includeQuery);

    default boolean isWrite()
    {
        return kind().isWrite();
    }

    default Result result(TxnId txnId, @Nullable Data data)
    {
        return query().compute(txnId, data, read(), update());
    }

    default Writes execute(Timestamp executeAt, @Nullable Data data)
    {
        Update update = update();
        if (update == null)
            return new Writes(executeAt, Keys.EMPTY, null);

        return new Writes(executeAt, update.keys(), update.apply(data));
    }

    default AsyncChain<Data> read(SafeCommandStore safeStore, Command command)
    {
        KeyRanges ranges = safeStore.ranges().at(command.executeAt().epoch);
        List<AsyncChain<Data>> futures = read().keys().foldl(ranges, (index, key, accumulate) -> {
            if (!safeStore.commandStore().hashIntersects(key))
                return accumulate;

            AsyncChain<Data> result = read().read(key, kind(), safeStore, command.executeAt(), safeStore.dataStore());
            accumulate.add(result);
            return accumulate;
        }, new ArrayList<>());
        return AsyncChains.reduce(futures, Data::merge);
    }
}
