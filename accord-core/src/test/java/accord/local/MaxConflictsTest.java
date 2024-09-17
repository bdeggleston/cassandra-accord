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
package accord.local;

import accord.api.RoutingKey;
import accord.primitives.*;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.Objects;

import static accord.local.MaxConflictsTest.Key.range;
import static org.junit.jupiter.api.Assertions.*;

public class MaxConflictsTest
{
    @Test
    public void testUpdateEmpty()
    {
        MaxConflicts empty =
            MaxConflicts.EMPTY;
        MaxConflicts updated =
            empty.update(Key.keys(5, 10, 15), ts(1));

        assertEquals(ts(1), updated.get(rk(5)));
        assertEquals(ts(1), updated.get(rk(10)));
        assertEquals(ts(1), updated.get(rk(15)));

        assertNull(updated.get(rk(4)));
        assertNull(updated.get(rk(8)));
        assertNull(updated.get(rk(12)));
        assertNull(updated.get(rk(16)));
    }

    @Test
    public void testUpdateWithEmpty()
    {
        MaxConflicts empty =
                MaxConflicts.EMPTY;
        MaxConflicts updated =
                empty.update(Key.keys(5, 10, 15), ts(1));
        MaxConflicts updatedAgain =
                updated.update(Key.keys(new int[0]), ts(1));

        assertSame(updated, updatedAgain);
    }

    @Test
    public void testUpdateStartOverlap()
    {
        MaxConflicts conflicts = MaxConflicts.EMPTY;

        conflicts = conflicts.update(Ranges.of(range(5,  10)), ts(2));
        conflicts = conflicts.update(Ranges.of(range(15, 20)), ts(4));
        conflicts = conflicts.update(Ranges.of(range(25, 30)), ts(6));
        conflicts = conflicts.update(Ranges.of(range(35, 40)), ts(8));

        conflicts = conflicts.update(Ranges.of(range(0, 22)), ts(3));

        assertEquals(ts(3), conflicts.get(rk(2)));
        assertEquals(ts(3), conflicts.get(rk(5)));
        assertEquals(ts(3), conflicts.get(rk(7)));
        assertEquals(ts(3), conflicts.get(rk(10)));
        assertEquals(ts(3), conflicts.get(rk(12)));

        assertEquals(ts(4), conflicts.get(rk(15)));
        assertEquals(ts(4), conflicts.get(rk(17)));
        assertEquals(ts(3), conflicts.get(rk(20)));
        assertEquals(ts(3), conflicts.get(rk(21)));
    }

    @Test
    public void testUpdateFullOverlap()
    {
        MaxConflicts conflicts = MaxConflicts.EMPTY;

        conflicts = conflicts.update(Ranges.of(range(5,  10)), ts(2));
        conflicts = conflicts.update(Ranges.of(range(15, 20)), ts(4));
        conflicts = conflicts.update(Ranges.of(range(25, 30)), ts(6));
        conflicts = conflicts.update(Ranges.of(range(35, 40)), ts(8));

        conflicts = conflicts.update(Ranges.of(range(0, 45)), ts(5));

        assertEquals(ts(5), conflicts.get(rk(0)));
        assertEquals(ts(5), conflicts.get(rk(7)));
        assertEquals(ts(5), conflicts.get(rk(11)));
        assertEquals(ts(5), conflicts.get(rk(14)));
        assertEquals(ts(6), conflicts.get(rk(27)));
        assertEquals(ts(5), conflicts.get(rk(30)));
        assertEquals(ts(5), conflicts.get(rk(31)));
        assertEquals(ts(5), conflicts.get(rk(32)));
        assertEquals(ts(8), conflicts.get(rk(37)));
        assertEquals(ts(5), conflicts.get(rk(42)));
    }

    @Test
    public void testUpdateSomeOverlap()
    {
        MaxConflicts conflicts = MaxConflicts.EMPTY;

        conflicts = conflicts.update(Ranges.of(range(5,  10)), ts(2));
        conflicts = conflicts.update(Ranges.of(range(15, 20)), ts(4));
        conflicts = conflicts.update(Ranges.of(range(25, 30)), ts(6));
        conflicts = conflicts.update(Ranges.of(range(35, 40)), ts(8));

        conflicts = conflicts.update(Ranges.of(range(12, 32)), ts(5));

        assertEquals(ts(2), conflicts.get(rk(7)));
        assertEquals(null,  conflicts.get(rk(11)));
        assertEquals(ts(5), conflicts.get(rk(14)));
        assertEquals(ts(6), conflicts.get(rk(27)));
        assertEquals(ts(5), conflicts.get(rk(30)));
        assertEquals(ts(5), conflicts.get(rk(31)));
        assertEquals(null,  conflicts.get(rk(32)));
        assertEquals(ts(8), conflicts.get(rk(37)));
    }

    @Test
    public void testUpdatePrependRange()
    {
        MaxConflicts empty =
                MaxConflicts.EMPTY;
        MaxConflicts updated =
                empty.update(Key.keys(5, 10, 15), ts(1));
        MaxConflicts updatedAgain =
                updated.update(Ranges.of(range(0,2), range(2,4)), ts(2));

        assertEquals(ts(1), updatedAgain.get(rk(5)));
        assertEquals(ts(1), updatedAgain.get(rk(10)));
        assertEquals(ts(1), updatedAgain.get(rk(15)));

        assertEquals(ts(2), updatedAgain.get(rk(0)));
        assertEquals(ts(2), updatedAgain.get(rk(1)));
        assertEquals(ts(2), updatedAgain.get(rk(2)));
        assertEquals(ts(2), updatedAgain.get(rk(3)));
    }

    @Test
    public void testUpdateAppendOne()
    {
        MaxConflicts empty =
                MaxConflicts.EMPTY;
        MaxConflicts updated =
                empty.update(Key.keys(5, 10, 15), ts(1));
        MaxConflicts updatedAgain =
                updated.update(Key.keys(16), ts(2));

        assertEquals(ts(1), updatedAgain.get(rk(5)));
        assertEquals(ts(1), updatedAgain.get(rk(10)));
        assertEquals(ts(1), updatedAgain.get(rk(15)));
        assertEquals(ts(2), updatedAgain.get(rk(16)));
    }

    @Test
    public void testUpdateExisting()
    {
        MaxConflicts empty =
                MaxConflicts.EMPTY;
        MaxConflicts updated =
                empty.update(Key.keys(5, 10, 15), ts(1));
        MaxConflicts updatedAgain =
                updated.update(Key.keys(5, 10, 15), ts(2));

        assertEquals(ts(2), updatedAgain.get(rk(5)));
        assertEquals(ts(2), updatedAgain.get(rk(10)));
        assertEquals(ts(2), updatedAgain.get(rk(15)));
    }

    @Test
    public void testUpdateAllNew()
    {
        MaxConflicts empty =
                MaxConflicts.EMPTY;
        MaxConflicts updated =
                empty.update(Key.keys(5, 10, 15), ts(1));
        MaxConflicts updatedAgain =
                updated.update(Key.keys(7, 12, 17), ts(2));

        assertEquals(ts(1), updatedAgain.get(rk(5)));
        assertEquals(ts(1), updatedAgain.get(rk(10)));
        assertEquals(ts(1), updatedAgain.get(rk(15)));

        assertEquals(ts(2), updatedAgain.get(rk(7)));
        assertEquals(ts(2), updatedAgain.get(rk(12)));
        assertEquals(ts(2), updatedAgain.get(rk(17)));
    }

    @Test
    public void testUpdateOldAndNew()
    {
        MaxConflicts empty =
                MaxConflicts.EMPTY;
        MaxConflicts updated =
                empty.update(Key.keys(5, 10, 15), ts(1));
        MaxConflicts updatedAgain =
                updated.update(Key.keys(5, 7, 10, 12, 15, 17), ts(2));

        assertEquals(ts(2), updatedAgain.get(rk(5)));
        assertEquals(ts(2), updatedAgain.get(rk(10)));
        assertEquals(ts(2), updatedAgain.get(rk(15)));

        assertEquals(ts(2), updatedAgain.get(rk(7)));
        assertEquals(ts(2), updatedAgain.get(rk(12)));
        assertEquals(ts(2), updatedAgain.get(rk(17)));
    }

    @Test
    public void testInsertAdjacentKeysBefore()
    {
        MaxConflicts empty =
                MaxConflicts.EMPTY;
        MaxConflicts updated =
                empty.update(Key.keys(5, 10, 15), ts(1));
        MaxConflicts updatedAgain =
                updated.update(Key.keys(4, 9, 14), ts(2));

        assertEquals(ts(1), updatedAgain.get(rk(5)));
        assertEquals(ts(1), updatedAgain.get(rk(10)));
        assertEquals(ts(1), updatedAgain.get(rk(15)));

        assertEquals(ts(2), updatedAgain.get(rk(4)));
        assertEquals(ts(2), updatedAgain.get(rk(9)));
        assertEquals(ts(2), updatedAgain.get(rk(14)));
    }

    @Test
    public void testInsertAdjacentKeysAfter()
    {
        MaxConflicts empty =
                MaxConflicts.EMPTY;
        MaxConflicts updated =
                empty.update(Key.keys(5, 10, 15), ts(1));
        MaxConflicts updatedAgain =
                updated.update(Key.keys(6, 11, 16), ts(2));

        assertEquals(ts(1), updatedAgain.get(rk(5)));
        assertEquals(ts(1), updatedAgain.get(rk(10)));
        assertEquals(ts(1), updatedAgain.get(rk(15)));

        assertEquals(ts(2), updatedAgain.get(rk(6)));
        assertEquals(ts(2), updatedAgain.get(rk(11)));
        assertEquals(ts(2), updatedAgain.get(rk(16)));
    }

    private static RoutingKey rk(int t)
    {
        return new Key.Routing(t);
    }

    private static Range r(int l, int r)
    {
        return range(l, r);
    }

    private static Timestamp ts(int b)
    {
        return Timestamp.fromValues(1, b, 0, new Node.Id(1));
    }

    public static class Key implements RoutableKey
    {
        public static class Raw extends Key implements accord.api.Key
        {
            public Raw(int key)
            {
                super(key);
            }

            @Override
            public accord.primitives.Range asRange()
            {
                return new Range(new Routing(key), new Routing(key + 1));
            }
        }

        public static class Routing extends Key implements accord.api.RoutingKey
        {
            public Routing(int key)
            {
                super(key);
            }

            @Override
            public accord.primitives.Range asRange()
            {
                return new Range(new Routing(key), new Routing(key + 1));
            }

            @Override
            public RangeFactory rangeFactory()
            {
                return (s, e) -> new Range((Routing)s, (Routing)e);
            }
        }

        public static class Range extends accord.primitives.Range.StartInclusive
        {
            public Range(Key.Routing start, Key.Routing end)
            {
                super(start, end);
            }

            @Override
            public accord.primitives.Range newRange(RoutingKey start, RoutingKey end)
            {
                return new Key.Range((Key.Routing)start, (Key.Routing)end);
            }
        }

        public final int key;

        public Key(int key)
        {
            this.key = key;
        }

        @Override
        public int compareTo(@Nonnull RoutableKey that)
        {
            return Integer.compare(this.key, ((Key)that).key);
        }

        public static Key.Raw key(int k)
        {
            return new Key.Raw(k);
        }

        public static Key.Routing routing(int k)
        {
            return new Key.Routing(k);
        }

        public static Keys keys(int k0, int... kn)
        {
            Key.Raw[] keys = new Key.Raw[kn.length + 1];
            keys[0] = new Key.Raw(k0);
            for (int i=0; i<kn.length; i++)
                keys[i + 1] = new Key.Raw(kn[i]);

            return Keys.of(keys);
        }

        public static Keys keys(int[] keyArray)
        {
            Key.Raw[] keys = new Key.Raw[keyArray.length];
            for (int i=0; i<keyArray.length; i++)
                keys[i] = new Key.Raw(keyArray[i]);

            return Keys.of(keys);
        }

        public static accord.primitives.Range range(Key.Routing start, Key.Routing end)
        {
            return new Key.Range(start, end);
        }

        public static accord.primitives.Range range(int start, int end)
        {
            return range(routing(start), routing(end));
        }

        @Override
        public String toString()
        {
            return Integer.toString(key);
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Key intKey = (Key) o;
            return key == intKey.key;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(key);
        }

        @Override
        public RoutingKey toUnseekable()
        {
            if (this instanceof Key.Routing)
                return (Key.Routing)this;
            return new Key.Routing(key);
        }
    }
}
