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

import accord.api.RoutingKey;
import accord.utils.Invariants;

import javax.annotation.Nonnull;

import static accord.primitives.AbstractRanges.UnionMode.MERGE_OVERLAPPING;
import static accord.primitives.Routables.Slice.Overlapping;

public abstract class RangeRoute extends AbstractRanges<Route<Range>> implements Route<Range>
{
    public final RoutingKey homeKey;

    RangeRoute(@Nonnull RoutingKey homeKey, Range[] ranges)
    {
        super(ranges);
        this.homeKey = Invariants.nonNull(homeKey);
    }

    @Override
    public Unseekables<Range, ?> with(Unseekables<Range, ?> with)
    {
        if (isEmpty())
            return with;

        return union(MERGE_OVERLAPPING, this, (AbstractRanges<?>) with, null, null,
                (left, right, rs) -> Ranges.ofSortedAndDeoverlapped(rs));
    }

    public Unseekables<Range, ?> with(RoutingKey withKey)
    {
        if (contains(withKey))
            return this;

        return with(Ranges.of(withKey.asRange()));
    }

    @Override
    public PartialRangeRoute slice(Ranges ranges)
    {
        return slice(ranges, Overlapping, this, homeKey, PartialRangeRoute::new);
    }

    @Override
    public RoutingKey homeKey()
    {
        return homeKey;
    }

    @Override
    public boolean equals(Object that)
    {
        return super.equals(that) && homeKey.equals(((RangeRoute)that).homeKey);
    }
}