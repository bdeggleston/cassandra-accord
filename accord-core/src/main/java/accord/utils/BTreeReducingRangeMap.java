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
package accord.utils;

import accord.api.RoutingKey;
import accord.primitives.*;
import accord.utils.btree.BTree;

import java.util.Comparator;
import java.util.Iterator;
import java.util.function.BiFunction;
import java.util.function.Predicate;

import static accord.utils.Invariants.checkArgument;
import static accord.utils.Invariants.checkState;
import static accord.utils.SortedArrays.Search.FAST;

public class BTreeReducingRangeMap<V> extends BTreeReducingIntervalMap<RoutingKey, V>
{
    public interface ReduceFunction<V, V2, P1, P2>
    {
        V2 apply(V v, V2 v2, P1 p1, P2 p2, int matchingStartBound);
    }

    public static class SerializerSupport
    {
        public static <V> BTreeReducingRangeMap<V> create(boolean inclusiveEnds, Object[] tree)
        {
            return new BTreeReducingRangeMap<>(inclusiveEnds, tree);
        }

        public static <V, M extends BTreeReducingRangeMap<V>> RawBuilder<V, M> rawBuilder(boolean inclusiveEnds, int capacity)
        {
            return new RawBuilder<>(inclusiveEnds, capacity);
        }

        public static <V> Iterator<Entry<RoutingKey, V>> rawIterator(BTreeReducingRangeMap<V> map)
        {
            return map.entriesIterator();
        }
    }

    public BTreeReducingRangeMap()
    {
        super();
    }

    protected BTreeReducingRangeMap(boolean inclusiveEnds, Object[] tree)
    {
        super(inclusiveEnds, tree);
    }

    public V foldl(Routables<?> routables, BiFunction<V, V, V> fold, V accumulator)
    {
        return foldl(routables, fold, accumulator, ignore -> false);
    }

    public <V2> V2 foldl(Routables<?> routables, BiFunction<V, V2, V2> fold, V2 accumulator, Predicate<V2> terminate)
    {
        return foldl(routables, (a, b, f, ignore) -> f.apply(a, b), accumulator, fold, null, terminate);
    }

    public <V2> V2 foldlWithBounds(Routables<?> routables, QuadFunction<V, V2, RoutingKey, RoutingKey, V2> fold, V2 accumulator, Predicate<V2> terminate)
    {
        return foldl(routables, (a, b, f, self, k) -> f.apply(a, b, self.entryAt(k).start(), self.entryAt(k+1).start()), accumulator, fold, this, terminate);
    }

    public <V2, P1> V2 foldl(Routables<?> routables, TriFunction<V, V2, P1, V2> fold, V2 accumulator, P1 p1, Predicate<V2> terminate)
    {
        return foldl(routables, (a, b, f, p) -> f.apply(a, b, p), accumulator, fold, p1, terminate);
    }

    public <V2, P1, P2> V2 foldl(Routables<?> routables, QuadFunction<V, V2, P1, P2, V2> fold, V2 accumulator, P1 p1, P2 p2, Predicate<V2> terminate)
    {
        return foldl(routables, (v, v2, param1, param2, k) -> fold.apply(v, v2, param1, param2), accumulator, p1, p2, terminate);
    }

    public <V2, P1> V2 foldlWithKey(Routables<?> routables, TriFunction<V, V2, P1, V2> fold, V2 accumulator, P1 p1, Predicate<V2> terminate)
    {
        return foldl(routables, (v, v2, f, p, k) -> f.apply(v, v2, p), accumulator, fold, p1, terminate);
    }

    public V foldlWithDefault(Routables<?> routables, BiFunction<V, V, V> fold, V defaultValue, V accumulator)
    {
        return foldlWithDefault(routables, fold, defaultValue, accumulator, ignore -> false);
    }

    public <V2> V2 foldlWithDefault(Routables<?> routables, BiFunction<V, V2, V2> fold, V defaultValue, V2 accumulator, Predicate<V2> terminate)
    {
        return foldlWithDefault(routables, (a, b, f, ignore) -> f.apply(a, b), defaultValue, accumulator, fold, null, terminate);
    }

    public <V2, P1> V2 foldlWithDefault(Routables<?> routables, TriFunction<V, V2, P1, V2> fold, V defaultValue, V2 accumulator, P1 p1, Predicate<V2> terminate)
    {
        return foldlWithDefault(routables, (a, b, f, p) -> f.apply(a, b, p), defaultValue, accumulator, fold, p1, terminate);
    }

    public <V2, P1, P2> V2 foldl(Routables<?> routables, ReduceFunction<V, V2, P1, P2> fold, V2 accumulator, P1 p1, P2 p2, Predicate<V2> terminate)
    {
        switch (routables.domain())
        {
            default: throw new AssertionError("Unknown domain: " + routables.domain());
            case Key: return foldl((AbstractKeys<?>) routables, fold, accumulator, p1, p2, terminate);
            case Range: return foldl((AbstractRanges) routables, fold, accumulator, p1, p2, terminate);
        }
    }

    public <V2, P1, P2> V2 foldl(AbstractKeys<?> keys, ReduceFunction<V, V2, P1, P2> fold, V2 accumulator, P1 p1, P2 p2, Predicate<V2> terminate)
    {
        if (isEmpty())
            return accumulator;

        // TODO (desired): first searches should be binarySearch
        int i = 0, j = keys.findNext(0, entryAt(0).start(), FAST);
        if (j < 0) j = -1 - j;
        else if (inclusiveEnds) ++j;

        while (j < keys.size())
        {
            i = BTree.findIndex(tree, Comparator.naturalOrder(), Entry.make(keys.get(j)));
            if (i < 0) i = -2 - i;
            else if (inclusiveEnds) --i;

            if (i >= size())
                return accumulator;

            int nextj = keys.findNext(j, entryAt(i + 1).start(), FAST);
            if (nextj < 0) nextj = -1 -nextj;
            else if (inclusiveEnds) ++nextj;

            Entry<RoutingKey, V> entry = entryAt(i);
            if (j != nextj && entry.hasValue() && entry.value() != null)
            {
                accumulator = fold.apply(entry.value(), accumulator, p1, p2, i);
                if (terminate.test(accumulator))
                    return accumulator;
            }
            ++i;
            j = nextj;
        }
        return accumulator;
    }

    public <V2, P1, P2> V2 foldl(AbstractRanges ranges, ReduceFunction<V, V2, P1, P2> fold, V2 accumulator, P1 p1, P2 p2, Predicate<V2> terminate)
    {
        if (isEmpty())
            return accumulator;

        // TODO (desired): first searches should be binarySearch
        RoutingKey firstKey = startAt(0);
        int j = ranges.findNext(0, firstKey, FAST);
        if (j < 0) j = -1 - j;
        else if (inclusiveEnds && ranges.get(j).end().equals(firstKey)) ++j;

        int i = 0;
        while (j < ranges.size())
        {
            Range range = ranges.get(j);
            RoutingKey start = range.start();
            int nexti = BTree.findIndex(tree, Comparator.naturalOrder(), Entry.make(start));
            if (nexti < 0) i = Math.max(i, -2 - nexti);
            else if (nexti > i && !inclusiveStarts()) i = nexti - 1;
            else i = nexti;

            if (i >= size())
                return accumulator;

            RoutingKey key = startAt(i + 1);
            int toj, nextj = ranges.findNext(j, key, FAST);
            if (nextj < 0) toj = nextj = -1 -nextj;
            else
            {
                toj = nextj + 1;
                if (inclusiveEnds && ranges.get(nextj).end().equals(key))
                    ++nextj;
            }

            Entry<RoutingKey, V> entry = entryAt(i);
            if (toj > j && entry.hasValue() && entry.value() != null)
            {
                accumulator = fold.apply(entry.value(), accumulator, p1, p2, i);
                if (terminate.test(accumulator))
                    return accumulator;
            }
            ++i;
            j = nextj;
        }
        return accumulator;
    }

    public <V2, P1, P2> V2 foldlWithDefault(Routables<?> routables, QuadFunction<V, V2, P1, P2, V2> fold, V defaultValue, V2 accumulator, P1 p1, P2 p2, Predicate<V2> terminate)
    {
        switch (routables.domain())
        {
            default: throw new AssertionError("Unknown domain: " + routables.domain());
            case Key: return foldlWithDefault((AbstractKeys<?>) routables, fold, defaultValue, accumulator, p1, p2, terminate);
            case Range: return foldlWithDefault((AbstractRanges) routables, fold, defaultValue, accumulator, p1, p2, terminate);
        }
    }

    private <V2, P1, P2> V2 foldlWithDefault(AbstractKeys<?> keys, QuadFunction<V, V2, P1, P2, V2> fold, V defaultValue, V2 accumulator, P1 p1, P2 p2, Predicate<V2> terminate)
    {
        if (isEmpty() || keys.isEmpty())
            return fold.apply(defaultValue, accumulator, p1, p2);

        int i = 0, j = keys.findNext(0, entryAt(0).start(), FAST);
        if (j < 0) j = -1 - j;
        else if (inclusiveEnds) ++j;

        if (j > 0)
            accumulator = fold.apply(defaultValue, accumulator, p1, p2);

        while (j < keys.size())
        {
            i = BTree.findIndex(tree, Comparator.naturalOrder(), Entry.make(keys.get(j)));
            if (i < 0) i = -2 - i;
            else if (inclusiveEnds) --i;

            if (i >= size())
                return fold.apply(defaultValue, accumulator, p1, p2);

            int nextj = keys.findNext(j, entryAt(i + 1).start(), FAST);
            if (nextj < 0) nextj = -1 -nextj;
            else if (inclusiveEnds) ++nextj;

            if (j != nextj)
            {
                Entry<RoutingKey, V> entry = entryAt(i);
                V value = entry.value() != null ? entry.value() : defaultValue;

                accumulator = fold.apply(value, accumulator, p1, p2);
                if (terminate.test(accumulator))
                    return accumulator;
            }
            ++i;
            j = nextj;
        }
        return accumulator;
    }

    private <V2, P1, P2> V2 foldlWithDefault(AbstractRanges ranges, QuadFunction<V, V2, P1, P2, V2> fold, V defaultValue, V2 accumulator, P1 p1, P2 p2, Predicate<V2> terminate)
    {
        if (isEmpty() || ranges.isEmpty())
            return fold.apply(defaultValue, accumulator, p1, p2);

        // TODO (desired): first searches should be binarySearch
        RoutingKey firstKey = startAt(0);
        int j = ranges.findNext(0, firstKey, FAST);
        if (j < 0) j = -1 - j;
        else if (inclusiveEnds && ranges.get(j).end().equals(firstKey)) ++j;

        if (j > 0 || firstKey.compareTo(ranges.get(0).start()) > 0)
            accumulator = fold.apply(defaultValue, accumulator, p1, p2);

        int i = 0;
        while (j < ranges.size())
        {
            Range range = ranges.get(j);
            RoutingKey start = range.start();
            int nexti = BTree.findIndex(tree, Comparator.naturalOrder(), Entry.make(start));
            if (nexti < 0) i = Math.max(i, -2 - nexti);
            else if (nexti > i && !inclusiveStarts()) i = nexti - 1;
            else i = nexti;

            if (i >= size())
                return fold.apply(defaultValue, accumulator, p1, p2);

            RoutingKey key = startAt(i + 1);
            int toj, nextj = ranges.findNext(j, key, FAST);
            if (nextj < 0) toj = nextj = -1 -nextj;
            else
            {
                toj = nextj + 1;
                if (inclusiveEnds && ranges.get(nextj).end().equals(key))
                    ++nextj;
            }

            if (toj > j)
            {
                Entry<RoutingKey, V> entry = entryAt(i);
                V value = entry.value() != null ? entry.value() : defaultValue;

                accumulator = fold.apply(value, accumulator, p1, p2);
                if (terminate.test(accumulator))
                    return accumulator;
            }
            ++i;
            j = nextj;
        }

        return accumulator;
    }

    public static <V> BTreeReducingRangeMap<V> create(AbstractRanges ranges, V value)
    {
        checkArgument(value != null, "value is null");

        if (ranges.isEmpty())
            return new BTreeReducingRangeMap<>();

        return create(ranges, value, BTreeReducingRangeMap.Builder::new);
    }

    public static <V, M extends BTreeReducingRangeMap<V>> M create(Unseekables<?> keysOrRanges, V value, BoundariesBuilderFactory<RoutingKey, V, M> builder)
    {
        switch (keysOrRanges.domain())
        {
            default: throw new AssertionError("Unhandled domain: " + keysOrRanges.domain());
            case Range: return create((AbstractRanges) keysOrRanges, value, builder);
            case Key: return create((AbstractUnseekableKeys) keysOrRanges, value, builder);
        }
    }

    public static <V, M extends BTreeReducingRangeMap<V>> M create(Seekables<?, ?> keysOrRanges, V value, BoundariesBuilderFactory<RoutingKey, V, M> builder)
    {
        switch (keysOrRanges.domain())
        {
            default: throw new AssertionError("Unhandled domain: " + keysOrRanges.domain());
            case Range: return create((AbstractRanges) keysOrRanges, value, builder);
            case Key: return create((Keys) keysOrRanges, value, builder);
        }
    }

    public static <V, M extends BTreeReducingRangeMap<V>> M create(AbstractRanges ranges, V value, BoundariesBuilderFactory<RoutingKey, V, M> factory)
    {
        checkArgument(value != null, "value is null");

        AbstractBoundariesBuilder<RoutingKey, V, M> builder = factory.create(ranges.get(0).endInclusive(), ranges.size() * 2);
        for (Range cur : ranges)
        {
            builder.append(cur.start(), value, (a, b) -> { throw new IllegalStateException(); });
            builder.append(cur.end(), null, (a, b) -> { throw new IllegalStateException(); });
        }

        return builder.build();
    }

    public static <V, M extends BTreeReducingRangeMap<V>> M create(AbstractUnseekableKeys keys, V value, BoundariesBuilderFactory<RoutingKey, V, M> factory)
    {
        checkArgument(value != null, "value is null");

        AbstractBoundariesBuilder<RoutingKey, V, M> builder = factory.create(keys.get(0).asRange().endInclusive(), keys.size() * 2);
        for (int i = 0 ; i < keys.size() ; ++i)
        {
            Range range = keys.get(i).asRange();
            builder.append(range.start(), value, (a, b) -> { throw new IllegalStateException(); });
            builder.append(range.end(), null, (a, b) -> { throw new IllegalStateException(); });
        }

        return builder.build();
    }

    public static <V, M extends BTreeReducingRangeMap<V>> M create(Keys keys, V value, BoundariesBuilderFactory<RoutingKey, V, M> factory)
    {
        checkArgument(value != null, "value is null");

        RoutingKey prev = keys.get(0).toUnseekable();
        AbstractBoundariesBuilder<RoutingKey, V, M> builder;
        {
            Range range = prev.asRange();
            builder = factory.create(prev.asRange().endInclusive(), keys.size() * 2);
            builder.append(range.start(), value, (a, b) -> { throw new IllegalStateException(); });
            builder.append(range.end(), null, (a, b) -> { throw new IllegalStateException(); });
        }

        for (int i = 1 ; i < keys.size() ; ++i)
        {
            RoutingKey unseekable = keys.get(i).toUnseekable();
            if (unseekable.equals(prev))
                continue;

            Range range = unseekable.asRange();
            builder.append(range.start(), value, (a, b) -> { throw new IllegalStateException(); });
            builder.append(range.end(), null, (a, b) -> { throw new IllegalStateException(); });
            prev = unseekable;
        }

        return builder.build();
    }

    public static <V> BTreeReducingRangeMap<V> add(BTreeReducingRangeMap<V> existing, Ranges ranges, V value, BiFunction<V, V, V> reduce)
    {
        BTreeReducingRangeMap<V> add = create(ranges, value);
        return merge(existing, add, reduce);
    }

    public static BTreeReducingRangeMap<Timestamp> add(BTreeReducingRangeMap<Timestamp> existing, Ranges ranges, Timestamp value)
    {
        return add(existing, ranges, value, Timestamp::max);
    }

    public static <V> BTreeReducingRangeMap<V> merge(BTreeReducingRangeMap<V> historyLeft, BTreeReducingRangeMap<V> historyRight, BiFunction<V, V, V> reduce)
    {
        return BTreeReducingIntervalMap.merge(historyLeft, historyRight, reduce, BTreeReducingRangeMap.Builder::new);
    }

    static class Builder<V> extends AbstractBoundariesBuilder<RoutingKey, V, BTreeReducingRangeMap<V>>
    {
        protected Builder(boolean inclusiveEnds, int capacity)
        {
            super(inclusiveEnds, capacity);
        }

        @Override
        protected BTreeReducingRangeMap<V> buildInternal(Object[] tree)
        {
            return new BTreeReducingRangeMap<>(inclusiveEnds, tree);
        }
    }

    /**
     * A non-validating builder that expects all entries to be in correct order. For implementations' ser/de logic.
     */
    public static class RawBuilder<V, M>
    {
        protected final boolean inclusiveEnds;
        protected final int capacity;

        private BTree.Builder<Entry<RoutingKey, V>> treeBuilder;
        private Entry<RoutingKey, V> lastAppended;
        private boolean lastStartAppended;

        public RawBuilder(boolean inclusiveEnds, int capacity)
        {
            this.inclusiveEnds = inclusiveEnds;
            this.capacity = capacity;
        }

        public Entry<RoutingKey, V> lastAppended()
        {
            return lastAppended;
        }

        public RawBuilder<V, M> append(RoutingKey start, V value)
        {
            return append(Entry.make(start, value));
        }

        public RawBuilder<V, M> append(RoutingKey start)
        {
            return append(Entry.make(start));
        }

        public RawBuilder<V, M> append(Entry<RoutingKey, V> entry)
        {
            checkState(!lastStartAppended);
            if (treeBuilder == null)
                treeBuilder = BTree.builder(Comparator.naturalOrder(), capacity + 1);
            treeBuilder.add(lastAppended = entry);
            lastStartAppended = !entry.hasValue();
            return this;
        }

        public final M build(BiFunction<Boolean, Object[], M> constructor)
        {
            checkState(lastStartAppended || treeBuilder == null);
            return constructor.apply(inclusiveEnds, treeBuilder == null ? BTree.empty() : treeBuilder.build());
        }
    }
}
