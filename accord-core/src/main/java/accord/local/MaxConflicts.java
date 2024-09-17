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

import javax.annotation.Nonnull;

import accord.api.RoutingKey;
import accord.primitives.AbstractRanges;
import accord.primitives.Routables;
import accord.primitives.Seekables;
import accord.primitives.Timestamp;
import accord.utils.BTreeReducingRangeMap;

// TODO (expected): track read/write conflicts separately
public class MaxConflicts extends BTreeReducingRangeMap<Timestamp>
{
    public static final MaxConflicts EMPTY = new MaxConflicts();

    private MaxConflicts()
    {
        super();
    }

    private MaxConflicts(boolean inclusiveEnds, Object[] tree)
    {
        super(inclusiveEnds, tree);
    }

    public Timestamp get(Seekables<?, ?> keysOrRanges)
    {
        return foldl(keysOrRanges, Timestamp::max, Timestamp.NONE);
    }

    public Timestamp get(Routables<?> keysOrRanges)
    {
        return foldl(keysOrRanges, Timestamp::max, Timestamp.NONE);
    }

    MaxConflicts update(Seekables<?, ?> keysOrRanges, Timestamp maxConflict)
    {
        return merge(this, create(keysOrRanges, maxConflict));
    }

    public static MaxConflicts create(AbstractRanges ranges, @Nonnull Timestamp maxConflict)
    {
        if (ranges.isEmpty())
            return EMPTY;

        return create(ranges, maxConflict, Builder::new);
    }

    public static MaxConflicts create(Seekables<?, ?> keysOrRanges, @Nonnull Timestamp maxConflict)
    {
        if (keysOrRanges.isEmpty())
            return MaxConflicts.EMPTY;

        return create(keysOrRanges, maxConflict, Builder::new);
    }

    public static MaxConflicts merge(MaxConflicts a, MaxConflicts b)
    {
        return merge(a, b, Timestamp::max, MaxConflicts.Builder::new);
    }

    static class Builder extends AbstractBoundariesBuilder<RoutingKey, Timestamp, MaxConflicts>
    {
        protected Builder(boolean inclusiveEnds, int capacity)
        {
            super(inclusiveEnds, capacity);
        }

        @Override
        protected MaxConflicts buildInternal(Object[] tree)
        {
            return new MaxConflicts(inclusiveEnds, tree);
        }
    }
}
